/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//
// Created by Renming Qi on 18/3/22.
//

#include "AsyncPReaderCallback.h"
#include <algorithm>
#include <memory>
#include <thread>
#include <tuple>
#include <boost/beast/core/flat_buffer.hpp>
#include <boost/exception/diagnostic_information.hpp>
#include <google/protobuf/io/coded_stream.h>
#include "Logger.h"
#include "client/DataTransferProtocolSender.h"
#include "client/Metrics.h"
namespace Hdfs
{
namespace Internal
{
    namespace AsyncCb
    {
#define bodyBufferConsume(consumed) \
    { \
        bodyBuffer.consume(consumed); \
    }
        //    LOG(DEBUG2, "line %d consumed %d", __LINE__, (consumed));

#define bodyBufferCommit(length) \
    if ((length) != 0) \
    { \
        bodyBuffer.commit(length); \
    }
        //        LOG(DEBUG2, "line %d commit %d", __LINE__, length); \


#define CheckEc(ec) \
    { \
        if ((ec)) \
        { \
            handleError((ec)); \
            return; \
        } \
    }

#define CheckProcessing(readContext) \
    { \
        if (!(readContext)->shared->isTaskInProcessing()) \
        { \
            CheckEc(error_code(hdfs_async_errors::operation_cancelled)); \
        } \
    }
        // LOG(DEBUG2, "read %d is canceled at %s, packet: %d", index, __FUNCTION__, packetCount); \

#define ExceptionGuardStart try

#define ExceptionGuardEnd \
    catch (HdfsException & ex) \
    { \
        LOG(LOG_ERROR, \
            "[AsyncPReaderV2] %s:%d unknown hdfs exception: %s", \
            __FUNCTION__, \
            __LINE__, \
            datanode.formatAddress().c_str(), \
            ex.msg()); \
        CheckEc(hdfs_async_errors::invalid_data); \
        return; \
    } \
    catch (...) \
    { \
        std::exception_ptr p = std::current_exception(); \
        const std::string msg = boost::current_exception_diagnostic_information(); \
        LOG(WARNING, \
            "[AsyncPReaderV2] %s:%d unknown hdfs exception: %s", \
            __FUNCTION__, \
            __LINE__, \
            datanode.formatAddress().c_str(), \
            msg.c_str()); \
        rethrow_exception(p); \
    }

        //#define ExceptionGuardStart
        //#define ExceptionGuardEnd

        AsyncPReadContext::AsyncPReadContext(char * buf_, size_t readSize_)
            : readSize(readSize_), buf(buf_), mu(std::make_shared<std::mutex>()), cond(std::make_shared<std::condition_variable>())
        {
            assert(mu != nullptr);
            assert(cond != nullptr);
            assert(buf != nullptr);
        }
        std::shared_ptr<AsyncPReadSessionContext> AsyncPReadContext::createSession()
        {
            auto ret = std::make_shared<AsyncPReadSessionContext>();
            ret->shared = shared_from_this();
            return ret;
        }
        void AsyncPReadContext::copyBuffer(int dstOffset, const char * src, int len)
        {
            /* this function is called when we read the body of packet out and need to copy the contents of the packet into
             * the `this->buf` which is provided by the users.
             * if the task is already finished and this function happens to be called, we don't conduct copy since there is already
             * the needed data in `this->buf`.
            */

            //            LOG(WARNING, "%s dstOffset %d src %p len %d end %d", __FUNCTION__, dstOffset, src, len, dstOffset + len);
            assert(dstOffset + len <= this->readSize);

            // only one reader can operation on user's buf.
            std::unique_lock<std::mutex> lk(*this->mu);
            if (!this->isTaskInProcessing())
                return;
            if (dstOffset + len == this->readSize)
            {
                //                LOG(WARNING, "finished reading");
                this->setFinished();
            }
            memcpy(this->buf + dstOffset, src, len);
        }

        AsyncPReaderV2::AsyncPReaderV2(
            std::shared_ptr<FileSystemInter> filesystem,
            std::shared_ptr<LocatedBlock> eb,
            const DatanodeInfo & datanode,
            PeerCache & peerCache,
            int64_t start,
            int64_t len,
            const Token & token,
            const std::string & clientName,
            bool verify,
            std::shared_ptr<SessionConfig> conf)
            : sentStatus(false)
            , verify(verify)
            , binfo(std::move(eb))
            , datanode(datanode)
            , checksumSize(0)
            , chunkSize(0)
            , position(0)
            , size(0)
            , start(start)
            , len(len)
            , cursor(start)
            , endOffset(start + len)
            , token(token)
            , lastSeqNo(-1)
            , peerCache(peerCache)
            , filesystem(filesystem)
            , clientName(clientName)
            , conf(conf)

        {
            //            bodyBufferSize = std::max((int64_t)(128 * 1024), std::max((int64_t)(PacketHeader::GetPkgHeaderSize()), len));
        }

        AsyncPReaderV2::~AsyncPReaderV2()
        {
            if (conf->getEnableSocketCache() && sentStatus)
            {
                peerCache.addConnection<boost::beast::tcp_stream>(tcpStream, datanode);
            }
        }

        void AsyncPReaderV2::handleError(error_code ec)
        {
            if (ec != hdfs_async_errors::operation_cancelled)
            {
                LOG(DEBUG2,
                    "[AsyncPReaderV2] read %d to datanode %s error, %s",
                    index,
                    datanode.formatAddress().c_str(),
                    ec.message().c_str());
            }
            else
            {
                LOG(DEBUG3,
                    "[AsyncPReaderV2] read %d to datanode %s error, %s",
                    index,
                    datanode.formatAddress().c_str(),
                    ec.message().c_str());
            }

            // emit metrics for error codes.
            // ex: tag = boost.asio.2 | req = 1
            std::string tag
                = "value=" + std::string(ec.category().name()) + "." + std::to_string(ec.value()) + "|req=" + std::to_string(index);
            errorCallback(ec);
        }

        void AsyncPReaderV2::handleSucc()
        {
            //            assert(bodyBuffer.size() == 0);
            if (bodyBuffer.size() != 0 || readBytes != readContext->shared->readSize)
            {
                LOG(FATAL,
                    "must be a bug, bodyBuffer.size = %lu readBytes = %d required size = %lu",
                    bodyBuffer.size(),
                    readBytes,
                    readContext->shared->readSize);
            }
            std::string metricName = "hedgeread.succ" + std::to_string(index);
            readContext->shared->nread = readBytes;
            //            LOG(DEBUG2,
            //                "[AsyncPReaderV2] read %d succeeded with %d bytes, required %d bytes",
            //                index,
            //                readBytes,
            //                readContext->shared->readSize);
            succCallback(error_code());
        }
        void AsyncPReaderV2::initReadTask(AsyncPReadSessionContext::Ptr ctx)
        {
            auto self = shared_from_this();
            readContext = std::move(ctx);
            //            LOG(WARNING, "recive task with read size = %d", readContext->shared->readSize);
            if (delayTimer)
            {
                // the executing thread is not blocked during `async_wait`. Once the timer is fired,
                // the task will be re-scheduled by the boost asio scheduler.
                delayTimer->async_wait([this, self](error_code ec) {
                    CheckEc(ec);
                    CheckProcessing(readContext);
                    initReadTaskNoDelay();
                });
            }
            else
            {
                initReadTaskNoDelay();
            }
        }

        void AsyncPReaderV2::initReadTaskNoDelay()
        {
            ExceptionGuardStart
            {
                auto self = shared_from_this();

                if (conf->enableSocketCache)
                {
                    tcpStream = peerCache.getConnection<boost::beast::tcp_stream>(datanode);
                }
                if (tcpStream)
                {
                    handleSendReadOp();
                }
                else
                {
                    tcp::endpoint endpoint(address::from_string(datanode.getIpAddr()), datanode.getXferPort());
                    tcpStream = std::make_shared<boost::beast::tcp_stream>(
                        bnet::make_strand(Hdfs::Internal::AsyncCb::AsioGlobalContext::Instance()));
                    tcpStream->expires_after(std::chrono::milliseconds(conf->getRpcConnectTimeout()));
                    tcpStream->async_connect(endpoint, [this, self](error_code ec) {
                        CheckEc(ec);
                        LOG(DEBUG3, "[AsyncPReaderV2] connected to datanode %s", datanode.formatAddress().c_str());
                        // set no delay.
                        tcpStream->expires_never();
                        tcpStream->socket().set_option(boost::asio::ip::tcp::no_delay(true));
                        handleSendReadOp();
                    });
                }
            }
            ExceptionGuardEnd
        }

        void AsyncPReaderV2::handleSendReadOp()
        {
            ExceptionGuardStart
            {
                auto self = shared_from_this();
                CheckProcessing(readContext);
                error_code ec;
                ProtocolUtil::composeReadBlockHeader(*binfo, token, clientName.c_str(), start, len, &sendReadRawDataBuffer);
                int size = sendReadRawDataBuffer.getDataSize(0);
                auto buf = boost::asio::buffer(sendReadRawDataBuffer.getBuffer(0), size);
                tcpStream->expires_after(std::chrono::milliseconds(conf->getInputWriteTimeout()));
                bnet::async_write(*tcpStream, buf, [this, self, size](error_code ec, size_t length) {
                    CheckEc(ec);
                    tcpStream->expires_never();
                    assert(length == size);
                    // LOG(DEBUG2, "[AsyncPReaderV2] sent readop to datanode %s", datanode.formatAddress().c_str());
                    handleReceiveReadOp(ec, 0);
                });
            }
            ExceptionGuardEnd
            //            tcpStream->async_write_some(boost::asio::buffer(rawdata,size ), yield[ec]);
        }

        void AsyncPReaderV2::handleReceiveReadOp(error_code ec, size_t length)
        {
            ExceptionGuardStart
            {
                auto self = shared_from_this();
                CheckEc(ec);
                bodyBufferCommit(length);
                CheckProcessing(readContext);
                int readTarget = 4;
                if (bodyBuffer.size() < readTarget)
                {
                    auto b = bodyBuffer.prepare(readTarget - bodyBuffer.size());
                    tcpStream->expires_after(std::chrono::milliseconds(conf->getInputReadTimeout()));
                    tcpStream->async_read_some(b, [this, self](error_code ec, size_t length) {
                        tcpStream->expires_never();
                        handleReceiveReadOp(ec, length);
                    });
                    return;
                }

                uint32_t response_size;
                google::protobuf::io::CodedInputStream is(
                    static_cast<const uint8_t *>(bodyBuffer.cdata().data()), static_cast<int>(bodyBuffer.size()));
                bool readSucc = is.ReadVarint32(&response_size);

                if (!readSucc)
                {
                    auto b = bodyBuffer.prepare(readTarget - bodyBuffer.size());
                    tcpStream->expires_after(std::chrono::milliseconds(conf->getInputReadTimeout()));
                    tcpStream->async_read_some(b, [this, self](error_code ec, size_t length) {
                        tcpStream->expires_never();
                        handleReceiveReadOp(ec, length);
                    });
                    return;
                }
                else
                {
                    bodyBufferConsume(is.CurrentPosition());
                    handleReceiveReadOpProto(response_size, ec, 0);
                }
            }
            ExceptionGuardEnd
        }


        void AsyncPReaderV2::handleReceiveReadOpProto(uint32_t response_size, error_code ec, size_t length)
        {
            ExceptionGuardStart
            {
                auto self = shared_from_this();
                CheckEc(ec);
                bodyBufferCommit(length);
                CheckProcessing(readContext);

                if (bodyBuffer.size() < response_size)
                {
                    auto b = bodyBuffer.prepare(response_size - bodyBuffer.size());
                    tcpStream->expires_after(std::chrono::milliseconds(conf->getInputReadTimeout()));
                    tcpStream->async_read_some(b, [this, self, response_size](error_code ec, size_t length) {
                        tcpStream->expires_never();
                        handleReceiveReadOpProto(response_size, ec, length);
                    });
                    return;
                }

                BlockOpResponseProto resp;
                if (!resp.ParseFromArray(static_cast<const uint8_t *>(bodyBuffer.cdata().data()), response_size))
                {
                    LOG(WARNING, "[AsyncPReaderV2] fail to parse BlockOpResponseProto size %d ", response_size);
                    CheckEc(hdfs_async_errors::invalid_parse);
                    return;
                }
                bodyBufferConsume(response_size);

                if (resp.status() != Status::DT_PROTO_SUCCESS)
                {
                    std::string msg;

                    if (resp.has_message())
                    {
                        msg = resp.message();
                    }

                    if (resp.status() == Status::DT_PROTO_ERROR_ACCESS_TOKEN)
                    {
                        LOG(WARNING,
                            "[AsyncPReaderV2]: block's token is invalid. Datanode: %s, Block: %s",
                            datanode.formatAddress().c_str(),
                            binfo->toString().c_str());
                        CheckEc(hdfs_async_errors::invalid_token);
                        return;
                    }
                    else
                    {
                        LOG(WARNING,
                            "[AsyncPReaderV2]: Datanode return an error when sending read request to Datanode: %s, Block: %s, %s.",
                            datanode.formatAddress().c_str(),
                            binfo->toString().c_str(),
                            (msg.empty() ? "check Datanode's log for more information" : msg.c_str()));
                        CheckEc(hdfs_async_errors::rpc_failed);
                        return;
                    }
                    return;
                }


                const ReadOpChecksumInfoProto & checksumInfo = resp.readopchecksuminfo();
                const ChecksumProto & cs = checksumInfo.checksum();
                chunkSize = cs.bytesperchecksum();
                if (chunkSize < 0)
                {
                    LOG(WARNING,
                        "[AsyncPReaderV2] invalid chunk size: %d, expected range[0, %lld], Block: %s, from Datanode: %s",
                        chunkSize,
                        binfo->getNumBytes(),
                        binfo->toString().c_str(),
                        datanode.formatAddress().c_str());
                    CheckEc(hdfs_async_errors::invalid_response);
                    return;
                }


                switch (cs.type())
                {
                    case ChecksumTypeProto::CHECKSUM_NULL:
                        verify = false;
                        checksumSize = 0;
                        break;

                    case ChecksumTypeProto::CHECKSUM_CRC32:
                    case ChecksumTypeProto::CHECKSUM_CRC32C:
                        if (HWCrc32c::available())
                        {
                            checksum = shared_ptr<Checksum>(new HWCrc32c());
                        }
                        else
                        {
                            checksum = shared_ptr<Checksum>(new SWCrc32c());
                        }

                        checksumSize = sizeof(int32_t);
                        break;

                    default:
                        LOG(WARNING,
                            "[AsyncPReaderV2] cannot recognize checksum type: %d, Block: %s, from Datanode: %s",
                            static_cast<int>(cs.type()),
                            binfo->toString().c_str(),
                            datanode.formatAddress().c_str());
                        CheckEc(hdfs_async_errors::invalid_response);
                        return;
                }

                int64_t firstChunkOffset = checksumInfo.chunkoffset();

                if (firstChunkOffset < 0 || firstChunkOffset > cursor || firstChunkOffset <= cursor - chunkSize)
                {
                    LOG(LOG_ERROR,
                        "[AsyncPReaderV2] invalid first chunk offset: %lld, expected range[0, %lld], "
                        "Block: %s, from Datanode: %s",
                        firstChunkOffset,
                        cursor,
                        binfo->toString().c_str(),
                        datanode.formatAddress().c_str());
                    CheckEc(hdfs_async_errors::invalid_response);
                    return;
                }

                // LOG(DEBUG2, "[AsyncPReaderV2] received readop from datanode %s", datanode.formatAddress().c_str());
                handleReadPacketHeader(ec, 0);
            }
            ExceptionGuardEnd
        }

        void AsyncPReaderV2::handleReadPacketHeader(error_code ec, size_t length, bool isTrailing)
        {
            ExceptionGuardStart
            {
                auto self = shared_from_this();

                CheckEc(ec);
                bodyBufferCommit(length);
                if (!isTrailing)
                {
                    CheckProcessing(readContext);
                }


                static const int packetHeaderLen = PacketHeader::GetPkgHeaderSize(); // packetHeaderLen = 31
                if (bodyBuffer.size() < packetHeaderLen)
                {
                    auto b = bodyBuffer.prepare(packetHeaderLen - bodyBuffer.size());
                    tcpStream->expires_after(std::chrono::milliseconds(conf->getInputReadTimeout()));
                    tcpStream->async_read_some(b, [this, self, isTrailing](error_code ec, size_t length) {
                        tcpStream->expires_never();
                        handleReadPacketHeader(ec, length, isTrailing);
                    });
                    return;
                }

                std::shared_ptr<PacketHeader> retval = std::make_shared<PacketHeader>();
                try
                {
                    retval->readFields(static_cast<const char *>(bodyBuffer.cdata().data()), packetHeaderLen);
                }
                catch (HdfsIOException & ex)
                {
                    LOG(LOG_ERROR, "[AsyncPReaderV2] parse packet header failed, isTrailing = %d , exception: %s", isTrailing, ex.msg());
                    CheckEc(hdfs_async_errors::invalid_parse);
                    return;
                }
                lastHeader = retval;
                packetCount += 1;
                bodyBufferConsume(packetHeaderLen);
                assert(lastHeader->getDataLen() > 0 || lastHeader->getPacketLen() == sizeof(int32_t));
                if (!lastHeader->sanityCheck(lastSeqNo))
                {
                    LOG(LOG_ERROR,
                        "[AsyncPReaderV2]: Packet failed on sanity check for block %s from Datanode %s.",
                        binfo->toString().c_str(),
                        datanode.formatAddress().c_str());
                    CheckEc(hdfs_async_errors::invalid_response);
                    return;
                }
                if (!isTrailing)
                {
                    // LOG(DEBUG2, "[AsyncPReaderV2] read packet body, dataSize = %d", lastHeader->getDataLen());
                    handleReadPacketBody(lastHeader->getDataLen(), ec, 0);
                }
                else
                {
                    // LOG(DEBUG2, "[AsyncPReaderV2] read trailing packet");
                    if (!lastHeader->isLastPacketInBlock() || lastHeader->getDataLen() != 0)
                    {
                        handleSucc();
                        return;
                    }
                    handleSendStatus();
                }
            }
            ExceptionGuardEnd
        }
        void AsyncPReaderV2::handleReadPacketBody(int dataSize, error_code ec, size_t length)
        {
            ExceptionGuardStart
            {
                auto self = shared_from_this();
                CheckEc(ec);
                bodyBufferCommit(length);
                CheckProcessing(readContext);
                int pendingAhead = 0;
                if (dataSize > 0)
                {
                    int chunks = (dataSize + chunkSize - 1) / chunkSize;
                    int checksumLen = chunks * checksumSize;
                    size = checksumLen + dataSize;
                    assert(size == lastHeader->getPacketLen() - static_cast<int>(sizeof(int32_t)));

                    if (bodyBuffer.size() < size)
                    {
                        auto b = bodyBuffer.prepare(size - bodyBuffer.size());
                        tcpStream->async_read_some(
                            b, [this, self, dataSize](error_code ec, size_t length) { handleReadPacketBody(dataSize, ec, length); });
                        return;
                    }


                    lastSeqNo = lastHeader->getSeqno();

                    if (lastHeader->getPacketLen() != static_cast<int>(sizeof(int32_t)) + dataSize + checksumLen)
                    {
                        //                    LOG(WARNING,
                        //                        "[AsyncPReaderV2] Invalid Packet, packetLen is %d, dataSize is %d, checksum size is %d",
                        //                        lastHeader->getPacketLen(),
                        //                        dataSize,
                        //                        checksumLen);
                        CheckEc(hdfs_async_errors::invalid_response);
                        return;
                    }

                    //TODO(renming):: add verify.
                    if (verify)
                    {
                        ec = verifyChecksum(chunks);
                        if (ec)
                        {
                            CheckEc(ec);
                            return;
                        }
                    }
                    /*
                 * skip checksum
                 */
                    position = checksumLen;
                    /*
                 * the first packet we get may start at the position before we required
                 */
                    pendingAhead = cursor - lastHeader->getOffsetInBlock();
                    pendingAhead = pendingAhead > 0 ? pendingAhead : 0;
                    position += pendingAhead;
                    //                LOG(WARNING,
                    //                    "cursor = %d, offset = %d, checksumLen = %d, pendingAhead = %d, position = %d, size =%d ",
                    //                    cursor,
                    //                    lastHeader->getOffsetInBlock(),
                    //                    checksumLen,
                    //                    pendingAhead,
                    //                    position,
                    //                    size);
                    bodyBufferConsume(position);


                    // copy data to dst.
                    int pendingSuffix = 0;
                    {
                        //                    int32_t todo = len < size - position ? len : size - position;
                        assert(len >= readBytes);
                        int32_t todo = std::min(std::min(len, static_cast<int64_t>(size - position)), len - readBytes);
                        //                    LOG(WARNING,
                        //                        "size = %d position = %d size - position = %d len = %d  readBytes = %d , todo = %d",
                        //                        size,
                        //                        position,
                        //                        size - position,
                        //                        len,
                        //                        readBytes,
                        //                        todo);
                        readContext->shared->copyBuffer(readBytes, static_cast<const char *>(bodyBuffer.cdata().data()), todo);
                        readBytes += todo;
                        position += todo;
                        cursor += todo;
                        bodyBufferConsume(todo);
                        pendingSuffix = size - position;
                    }
                    // check trailing
                    {
                        if (lastHeader->getDataLen() + lastHeader->getOffsetInBlock() >= endOffset)
                        {
                            // there might be some remaining packet data in bodyBuffer.
                            bodyBufferConsume(pendingSuffix);
                            handleReadPacketHeader(ec, 0, true);
                        }
                        else
                        {
                            handleReadPacketHeader(ec, 0, false);
                        }
                    }
                }
            }
            ExceptionGuardEnd
        }

        void AsyncPReaderV2::handleSendStatus()
        {
            ExceptionGuardStart
            {
                auto self = shared_from_this();
                error_code ec;
                ClientReadStatusProto status;

                if (verify)
                {
                    status.set_status(Status::DT_PROTO_CHECKSUM_OK);
                }
                else
                {
                    status.set_status(Status::DT_PROTO_SUCCESS);
                }

                int size = status.ByteSize();
                sendStatusRawDataBuffer.writeVarint32(size);
                status.SerializeToArray(sendStatusRawDataBuffer.alloc(size), size);
                tcpStream->expires_after(std::chrono::milliseconds(conf->getInputWriteTimeout()));
                bnet::async_write(
                    *tcpStream,
                    boost::asio::buffer(sendStatusRawDataBuffer.getBuffer(0), sendStatusRawDataBuffer.getDataSize(0)),
                    [this, self](error_code ec, size_t length) {
                        tcpStream->expires_never();
                        sentStatus = true;
                        handleSucc();
                    });
            }
            ExceptionGuardEnd
        }

        void AsyncPReaderV2::setDelay(int64_t mill)
        {
            delayMills = mill;
            if (delayMills)
            {
                delayTimer = std::make_unique<boost::asio::steady_timer>(
                    Hdfs::Internal::AsyncCb::AsioGlobalContext::Instance(), std::chrono::milliseconds(mill));
            }
        }

        void AsyncPReaderV2::setIndex(int index_)
        {
            index = index_;
        }

        error_code AsyncPReaderV2::verifyChecksum(int chunks)
        {
            error_code ec;
            int dataSize = lastHeader->getDataLen();
            auto buffer = static_cast<const char *>(bodyBuffer.cdata().data());
            const char * pchecksum = &buffer[0];
            const char * pdata = &buffer[0] + (chunks * checksumSize);

            for (int i = 0; i < chunks; ++i)
            {
                int size = chunkSize < dataSize ? chunkSize : dataSize;
                dataSize -= size;
                checksum->reset();
                checksum->update(pdata + (i * chunkSize), size);
                uint32_t result = checksum->getValue();
                uint32_t target = ReadBigEndian32FromArray(pchecksum + (i * checksumSize));

                if (result != target)
                {
                    LOG(WARNING,
                        "[AsyncPReaderV2]: block's checksum is invalid. Datanode: %s, Block: %s, Chunk: %d",
                        datanode.formatAddress().c_str(),
                        binfo->toString().c_str(),
                        i);
                    return hdfs_async_errors::invalid_checksum;
                }
            }

            assert(0 == dataSize);
            return ec;
        }

    }
}
}
