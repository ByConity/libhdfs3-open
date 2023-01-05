/********************************************************************
 * 2014 -
 * open source under Apache License Version 2.0
 ********************************************************************/
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "InputStreamImpl.h"
#include <algorithm>
#include <iostream>
#include <memory>
#include <random>
#include <sstream>
#include <ifaddrs.h>
#include <inttypes.h>
#include <unistd.h>
#include <boost/asio.hpp>
#include <boost/asio/spawn.hpp>
#include <boost/fiber/all.hpp>
#include <sys/socket.h>
#include <sys/types.h>
#include "Exception.h"
#include "ExceptionInternal.h"
#include "FileSystemInter.h"
#include "InputStreamInter.h"
#include "LocalBlockReader.h"
#include "Logger.h"
#include "RemoteBlockReader.h"
#include "ScopeGuard.h"
#include "StopWatch.h"
#include "Thread.h"
#include "client/Metrics.h"
#include "client/async_preader/AsyncPReaderCallback.h"
#include "server/Datanode.h"
namespace Hdfs
{
namespace Internal
{

unordered_set<std::string> BuildLocalAddrSet()
{
    unordered_set<std::string> set;
        struct ifaddrs * ifAddr = NULL;
        struct ifaddrs * pifAddr = NULL;
        struct sockaddr * addr;

        if (getifaddrs(&ifAddr))
        {
            THROW(HdfsNetworkException, "InputStreamImpl: cannot get local network interface: %s", GetSystemErrorInfo(errno));
        }

        try
        {
            std::vector<char> host;
            const char * pHost;
            host.resize(INET6_ADDRSTRLEN + 1);

            for (pifAddr = ifAddr; pifAddr != NULL; pifAddr = pifAddr->ifa_next)
            {
                addr = pifAddr->ifa_addr;

                if (!addr)
                {
                    continue;
                }

                memset(&host[0], 0, INET6_ADDRSTRLEN + 1);

                if (addr->sa_family == AF_INET)
                {
                    pHost
                        = inet_ntop(addr->sa_family, &(reinterpret_cast<struct sockaddr_in *>(addr))->sin_addr,
                              &host[0], INET6_ADDRSTRLEN);
            } else if (addr->sa_family == AF_INET6) {
                pHost =
                    inet_ntop(addr->sa_family,
                              &(reinterpret_cast<struct sockaddr_in6 *>(addr))->sin6_addr,
                              &host[0], INET6_ADDRSTRLEN);
            } else {
                continue;
            }

            if (NULL == pHost) {
                THROW(HdfsNetworkException,
                      "InputStreamImpl: cannot get convert network address to textual form: %s",
                      GetSystemErrorInfo(errno));
            }

            set.insert(pHost);
        }

        /*
         * add hostname.
         */
        long hostlen = sysconf(_SC_HOST_NAME_MAX);
        host.resize(hostlen + 1);

        if (gethostname(&host[0], host.size())) {
            THROW(HdfsNetworkException,
                  "InputStreamImpl: cannot get hostname: %s",
                  GetSystemErrorInfo(errno));
        }

        set.insert(&host[0]);
    } catch (...) {
        if (ifAddr != NULL) {
            freeifaddrs(ifAddr);
        }

        throw;
    }

    if (ifAddr != NULL) {
        freeifaddrs(ifAddr);
    }

    return set;
}

InputStreamImpl::InputStreamImpl()
    : closed(true)
    , localRead(true)
    , readFromUnderConstructedBlock(false)
    , verify(true)
    , maxGetBlockInfoRetry(3)
    , cursor(0)
    , endOfCurBlock(0)
    , lastBlockBeingWrittenLength(0)
    , totalBytesRead(0)
    , prefetchSize(0)
    , peerCache(NULL)
    , nodeResolver(nullptr)
    , block_location_manager_(BlockLocationManager::GetSingleton(0))
    , hdfs_event_callback(nullptr)
    , nodeMutex()
{
#ifdef MOCK
    stub = NULL;
#endif
}

InputStreamImpl::~InputStreamImpl() {
    closeShared();
}

void InputStreamImpl::checkStatus() {
    if (closed) {
        THROW(HdfsIOException, "InputStreamImpl: stream is not opened.");
    }

    if (lastError != exception_ptr()) {
        rethrow_exception(lastError);
    }
}


int64_t InputStreamImpl::readBlockLength(const LocatedBlock & b) {
    const std::vector<DatanodeInfo> & nodes = b.getLocations();
    int replicaNotFoundCount = nodes.size();

    for (size_t i = 0; i < nodes.size(); ++i) {
        try {
            int64_t n = 0;
            shared_ptr<Datanode> dn;
            RpcAuth a = auth;
            a.getUser().addToken(b.getToken());
#ifdef MOCK

            if (stub) {
                dn = stub->getDatanode();
            } else {
                dn = shared_ptr < Datanode > (new DatanodeImpl(nodes[i].getIpAddr().c_str(),
                                              nodes[i].getIpcPort(), *conf, a));
            }

#else
            dn = shared_ptr < Datanode > (new DatanodeImpl(nodes[i].getIpAddr(),
                                          nodes[i].getIpcPort(), *conf, a));
#endif
            n = dn->getReplicaVisibleLength(b);

            if (n >= 0) {
                return n;
            }
        } catch (const ReplicaNotFoundException & e) {
            std::string buffer;
            LOG(LOG_ERROR,
                "InputStreamImpl: failed to get block visible length for Block: %s file %s from Datanode: %s\n%s",
                b.toString().c_str(), path.c_str(), nodes[i].formatAddress().c_str(), GetExceptionDetail(e, buffer));
            LOG(INFO,
                "InputStreamImpl: retry get block visible length for Block: %s file %s from other datanode",
                b.toString().c_str(), path.c_str());
            --replicaNotFoundCount;
        } catch (const HdfsIOException & e) {
            std::string buffer;
            LOG(LOG_ERROR,
                "InputStreamImpl: failed to get block visible length for Block: %s file %s from Datanode: %s\n%s",
                b.toString().c_str(), path.c_str(), nodes[i].formatAddress().c_str(), GetExceptionDetail(e, buffer));
            LOG(INFO,
                "InputStreamImpl: retry get block visible length for Block: %s file %s from other datanode",
                b.toString().c_str(), path.c_str());
        }
    }

    // Namenode told us about these locations, but none know about the replica
    // means that we hit the race between pipeline creation start and end.
    // we require all 3 because some other exception could have happened
    // on a DN that has it.  we want to report that error
    if (replicaNotFoundCount == 0) {
        return 0;
    }

    return -1;
}

/**
 * Getting blocks locations'information from namenode
 */


void InputStreamImpl::updateBlockInfos()
{
    int retry = maxGetBlockInfoRetry;
    bool location_cache_hit{false};
    if (enableLocationCache)
    {
        // should get a copy of cached entry so that there's no concurrency issues.
        auto location_cache = block_location_manager_.getBlockLocationCopy(path, cursor);
        if (location_cache.has_value())
        {
            location_cache_hit = true;
            lbs = location_cache.value(); // should copy instead of using the pointer in cache.
            LOG(DEBUG2, "[LOCATION CACHE] hit for %s", path.c_str());
        }
    }

    for (int i = 0; i < retry; ++i)
    {
        try
        {
            if (!lbs)
            {
                lbs = std::make_shared<LocatedBlocksImpl>();
            }
            if (!location_cache_hit)
            {
                LOG(DEBUG2, "[LOCATION CACHE] getBlockLocations %s", path.c_str());
                Stopwatch sw;
                filesystem->getBlockLocations(path, cursor, prefetchSize, *lbs);
                sw.stop();
                CNCH_HDFS_CALLBACK2(hdfs_event_callback,Event::HDFS_EVENT_GET_BLOCK_LOCATION, int64_t(sw.elapsedNanoseconds()));

                if (enableLocationCache && lbs->getBlocks().size() > 0 && !lbs->isUnderConstruction())
                {
                    auto lbs_copy = lbs->deep_copy();
                    // should pass a full copy into cache so that lbs itself will not be modified.
                    block_location_manager_.setBlockLocationCache(path, lbs_copy);
                }
            }
            if (lbs->isLastBlockComplete())
            {
                lastBlockBeingWrittenLength = 0;
            }
            else
            {
                shared_ptr<LocatedBlock> last = lbs->getLastBlock();

                if (!last)
                {
                    lastBlockBeingWrittenLength = 0;
                }
                else
                {
                    lastBlockBeingWrittenLength = readBlockLength(*last);

                    if (lastBlockBeingWrittenLength == -1)
                    {
                        if (i + 1 >= retry)
                        {
                            THROW(
                                HdfsIOException,
                                "InputStreamImpl: failed to get block visible length for Block: %s from all Datanode.",
                                last->toString().c_str());
                        }
                        else
                        {
                            LOG(LOG_ERROR,
                                "InputStreamImpl: failed to get block visible length for Block: %s file %s from all Datanode.",
                                last->toString().c_str(),
                                path.c_str());

                            try
                            {
                                sleep_for(milliseconds(4000));
                            }
                            catch (...)
                            {
                            }

                            continue;
                        }
                    }

                    last->setNumBytes(lastBlockBeingWrittenLength);
                }
            }

            return;
        } catch (const HdfsRpcException & e) {
            std::string buffer;
            LOG(LOG_ERROR,
                "InputStreamImpl: failed to get block information for file %s, %s",
                path.c_str(), GetExceptionDetail(e, buffer));

            if (i + 1 >= retry) {
                throw;
            }
        }

        LOG(INFO,
            "InputStreamImpl: retry to get block information for file: %s, already tried %d time(s).",
            path.c_str(), i + 1);
    }
}

int64_t InputStreamImpl::getFileLength() {
    int64_t length = lbs->getFileLength();

    if (!lbs->isLastBlockComplete()) {
        length += lastBlockBeingWrittenLength;
    }

    return length;
}

void InputStreamImpl::seekToBlock(const LocatedBlock & lb) {
    if (cursor >= lbs->getFileLength()) {
        assert(!lbs->isLastBlockComplete());
        readFromUnderConstructedBlock = true;
    } else {
        readFromUnderConstructedBlock = false;
    }

    assert(cursor >= lb.getOffset()
           && cursor < lb.getOffset() + lb.getNumBytes());
    curBlock = shared_ptr < LocatedBlock > (new LocatedBlock(lb));
    int64_t blockSize = curBlock->getNumBytes();
    assert(blockSize > 0);
    endOfCurBlock = blockSize + curBlock->getOffset();
    failedNodes.clear();
    blockReader.reset();
}


void InputStreamImpl::setSlowNode(const DatanodeInfo& node)
{
    LOG(DEBUG2, "Node: %s been detected marked slow", curNode.getHostName().c_str());
    if (conf->useBadNodeStore())
    {
        nodeResolver->setSlowNode(node);
    }
    {
        std::unique_lock lock(nodeMutex);
        slowNodes.emplace(node);
    }
}
void InputStreamImpl::setFailedNode(const DatanodeInfo & node)
{
    LOG(DEBUG2, "Node: %s been detected marked failed", curNode.getHostName().c_str());
    if (conf->useBadNodeStore())
    {
        nodeResolver->setFailNode(node);
    }
    {
        std::unique_lock lock(nodeMutex);
        failedNodes.emplace(node);
    }
}
void InputStreamImpl::setCurrentNode(DatanodeInfo node)
{
    //    if(!(curNode == node)) {
    //        throughputWindow.reset();
    //    }
    curNode = std::move(node);
}
bool InputStreamImpl::choseBestNode()
{
    const std::vector<DatanodeInfo> & nodes = curBlock->getLocations();

    std::vector<DatanodeInfo> availableNodes;
    int has_local = 0;
    for (const auto & node : nodes)
    {
        if (failedNodes.find(node) != failedNodes.end())
        {
            continue;
        }
        availableNodes.push_back(node);
    }


    {
        // shuffle availabe nodes
        thread_local static std::random_device rd;
        thread_local static std::mt19937 g(rd());
        if (availableNodes.size() > 1)
        {
            std::shuffle(availableNodes.begin() + has_local, availableNodes.end(), g);
        }
    }

    if (conf->useBadNodeStore())
    {
        // so the way this works, is the following:
        // Assume that we have no failedNodes at the moment, getBestNode should return the
        //   best possible node.
        //   There is a chance that the "best possible node" might be a node had previously failed and not expired
        //   however, in that case, we still have to try because we have no other replicas to choose from.
        //   so we still need to maintain our own failedNodes list here.
        // 2. In the event that availableNodes is empty, then getBestNode would have returned nullopt
        //   indicating that we have failed enough times.
        // We don't need to use any info from slowNodes, because getBestNode prioritizes good nodes over known bad nodes
        // good node > slow node > fail nodes.

        auto node = nodeResolver->getBestNode(availableNodes);
        if(node) {
            setCurrentNode(*node);
            return true;
        }
        return false;
    }

    for (size_t i = 0; i < availableNodes.size(); ++i)
    {
        if (slowNodes.find(availableNodes[i]) != slowNodes.end())
        {
            continue;
        }
        setCurrentNode(availableNodes[i]);
        return true;
    }

    if (!availableNodes.empty())
    {
        setCurrentNode(availableNodes[0]);
        if (conf->getEnableFastSwitchRead())
        {
            slowNodes.clear();
        }
        return true;
    }

    return false;
}

bool InputStreamImpl::isLocalNode()
{ return isLocalNode(curNode); }

bool InputStreamImpl::isLocalNode(const DatanodeInfo & node)
{
    static const unordered_set<std::string> LocalAddrSet = BuildLocalAddrSet();
    bool retval = LocalAddrSet.find(node.getIpAddr()) != LocalAddrSet.end();
    return retval;
}


void InputStreamImpl::setupBlockReader(bool temporaryDisableLocalRead, int32_t size, bool pread)
{
    bool lastReadFromLocal = false;
    exception_ptr lastException;

    while (true)
    {
        if (!choseBestNode())
        {
            try
            {
                if (lastException)
                {
                    rethrow_exception(lastException);
                }
            }
            catch (...)
            {
                NESTED_THROW(
                    HdfsIOException,
                    "InputStreamImpl: all nodes have been tried and no valid replica can be read for Block: %s.",
                    curBlock->toString().c_str());
            }

            THROW(
                HdfsIOException,
                "InputStreamImpl: all nodes have been tried and no valid replica can be read for Block: %s.",
                curBlock->toString().c_str());
        }

        try {
            int64_t offset, len;
            offset = cursor - curBlock->getOffset();
            assert(offset >= 0);
            len = curBlock->getNumBytes() - offset;
            if (pread)
            {
                len = len > size ? size : len;
            }
            assert(len > 0);

            if (!temporaryDisableLocalRead && !lastReadFromLocal &&
                !readFromUnderConstructedBlock && localRead && isLocalNode()) {
                lastReadFromLocal = true;

                shared_ptr<ReadShortCircuitInfo> info;
                ReadShortCircuitInfoBuilder builder(curNode, auth, *conf);
                EncryptionKey ekey = filesystem->getEncryptionKeys();

                try {
                    info = builder.fetchOrCreate(*curBlock, curBlock->getToken(), ekey);

                    if (!info) {
                        continue;
                    }

                    assert(info->isValid());
                    blockReader = shared_ptr<BlockReader>(
                        new LocalBlockReader(info, *curBlock, offset, verify,
                                             *conf, localReaderBuffer));
                } catch (...) {
                    if (info) {
                        info->setValid(false);
                    }

                    throw;
                }
            } else {
                const char * clientName = filesystem->getClientName();
                lastReadFromLocal = false;
                Stopwatch sw;
                SCOPE_EXIT(
                    { CNCH_HDFS_CALLBACK2(hdfs_event_callback, Event::HDFS_EVENT_CREATE_BLOCK_READER, int64_t(sw.elapsedNanoseconds())); };);
                blockReader = shared_ptr<BlockReader>(new RemoteBlockReader(
                    filesystem,
                    *curBlock, curNode, *peerCache, offset, len,
                    curBlock->getToken(), clientName, verify, *conf, switch_read_strategy_, hdfs_event_callback));
            }

            break;
        } catch (const HdfsException & e) {
            lastException = current_exception();
            std::string buffer;

            if (lastReadFromLocal) {
                LOG(LOG_ERROR,
                    "cannot setup block reader for Block: %s file %s on Datanode: %s.\n%s\n"
                    "retry the same node but disable read shortcircuit feature",
                    curBlock->toString().c_str(), path.c_str(),
                    curNode.formatAddress().c_str(), GetExceptionDetail(e, buffer));
                /*
                 * do not add node into failedNodes since we will retry the same node but
                 * disable local block reading
                 */
            } else {
                if (conf->getEncryptedDatanode() || conf->getSecureDatanode())
                    LOG(WARNING,
                        "cannot setup block reader for Block: %s file %s on Datanode: %s retry another node",
                        curBlock->toString().c_str(), path.c_str(),
                        curNode.formatAddress().c_str());
                else
                    LOG(LOG_ERROR,
                        "cannot setup block reader for Block: %s file %s on Datanode: %s.\n%s\nretry another node",
                        curBlock->toString().c_str(), path.c_str(),
                        curNode.formatAddress().c_str(), GetExceptionDetail(e, buffer));
                setFailedNode(curNode);
            }
        }
    }
}

void InputStreamImpl::open(shared_ptr<FileSystemInter> fs, const char * path,
                           bool verifyChecksum, hdfsEventCallBack cb) {
    this->enableLocationCache = fs->getConf().getEnableBlockLocationCache();
    this->enableHedgedRead = fs->getConf().getEnableHedgedRead();
    this->hdfs_event_callback = cb;
    this->verify = fs->getConf().getVerifyChecksum();
    if (NULL == path || 0 == strlen(path)) {
        THROW(InvalidParameter, "path is invalid.");
    }

    try {
        openInternal(fs, path, verifyChecksum);
    } catch (...) {
        close();
        throw;
    }
}


void InputStreamImpl::openInternal(shared_ptr<FileSystemInter> fs, const char * path,
                                   bool verifyChecksum) {
    try {
        filesystem = fs;
        verify = verifyChecksum;
        this->path = fs->getStandardPath(path);
        LOG(DEBUG2, "%p, open file %s for read, verfyChecksum is %s", this, this->path.c_str(), (verifyChecksum ? "true" : "false"));
        conf = shared_ptr<SessionConfig>(new SessionConfig(fs->getConf()));
        this->auth = RpcAuth(fs->getUserInfo(), RpcAuth::ParseMethod(conf->getRpcAuthMethod()));
        prefetchSize = conf->getDefaultBlockSize() * conf->getPrefetchSize();
        localRead = conf->isReadFromLocal();
        maxGetBlockInfoRetry = conf->getMaxGetBlockInfoRetry();
        peerCache = &fs->getPeerCache();
        nodeResolver = &fs->getNodeResolver();
        updateBlockInfos();
        closed = false;
        // disable switch read when hedged read is enabled.
        if (conf->getEnableFastSwitchRead() && !conf->getEnableHedgedRead())
        {
            //            std::stringstream ss;
            //            ss << "conf->getFSRSowIOThreshold(): " << conf->getFSRSlowIOThreshold()
            //               << ". conf->getFSRIOTimeout(): " << conf->getFSRIOTimeout()
            //               << ". conf->getFSRTimeWindowLengthNS(): " << conf->getFSRTimeWindowLengthNS()
            //               << ". conf->getFSRMinimumTimeNS(): " << conf->getFSRMinimumTimeNS();
            //            LOG(INFO, "%s", ss.str().c_str());
            switch_read_strategy_ = std::make_shared<FastSwitchReadStrategy>(
                conf->getFSRSlowIOThreshold(),
                conf->getFSRIOTimeout(),
                std::chrono::nanoseconds(conf->getFSRTimeWindowLengthNS()),
                std::chrono::nanoseconds(conf->getFSRMinimumTimeNS()),
                conf);
        }
    }
    catch (const HdfsException & e) {
        LOG(LOG_ERROR, "InputStreamImpl(%d): %s.",  __LINE__, e.msg());
        throw;
    }
}

int32_t InputStreamImpl::read(char * buf, int32_t size)
{
    checkStatus();
    try
    {
        int64_t prvious = cursor;
        int32_t done = readInternal(buf, size, false);
        LOG(DEBUG2,
            "%p read file %s size is %d, offset %" PRId64 " done %d, next pos %" PRId64,
            this,
            path.c_str(),
            size,
            prvious,
            done,
            cursor);
        return done;
    }
    catch (const HdfsEndOfStream & e)
    {
        throw;
    }
    catch (...)
    {
        block_location_manager_.removeBlockLocationCache(path);
        lastError = current_exception();
        throw;
    }
}

int32_t InputStreamImpl::pread(char * buf, int32_t size)
{
    checkStatus();
    try
    {
        int64_t prvious = cursor;
        int32_t done = readInternal(buf, size, true);
        LOG(DEBUG3,
            "%p read file %s size is %d, offset %" PRId64 " done %d, next pos %" PRId64,
            this,
            path.c_str(),
            size,
            prvious,
            done,
            cursor);
        return done;
    }
    catch (const HdfsEndOfStream & e)
    {
        throw;
    }
    catch (...)
    {
        lastError = current_exception();
        throw;
    }
}

/*
 * This function will read data within one block into `buf`.
 * The read length is the min of the provided `size` and the number of remaining bytes in this block.
 * It will be executed in boost's io_context (which basically, could be viewed as a thread pool).
 * Check more at Internal::AsyncCb::AsyncPReaderV2::InitRead.
 */
int32_t InputStreamImpl::readOneBlockAsyncPReadV2(char * buf, int32_t size, bool shouldUpdateMetaDataOnFailure)
{
    static const std::string ASYNC_CLIENT_NAME{"hdfs_ck_async"};
    // calculate the size to be read.
    int32_t toRead = (size < endOfCurBlock - cursor ? size : static_cast<int32_t>(endOfCurBlock - cursor));
    //  offset is offset in current block. len should be equal to toRead.
    int64_t offset, len;
    offset = cursor - curBlock->getOffset();
    assert(offset >= 0);
    len = curBlock->getNumBytes() - offset;
    len = std::min(len, static_cast<int64_t>(size));


    std::vector<DatanodeInfo> sortedLocations;
    {
        // we will store all datanodes info into sortedLocations and put the best node at head.
        const std::vector<DatanodeInfo> & allLocations = curBlock->getLocations();
        auto hasBestNode = choseBestNode();
        if (hasBestNode)
        {
            sortedLocations.push_back(curNode);
            for (int i = 0; i < allLocations.size(); i++)
            {
                if (curNode != allLocations[i])
                {
                    sortedLocations.push_back(allLocations[i]);
                }
            }
        }
        else
        {
            sortedLocations = allLocations;
        }
    }

    // create the pread context for this read operation.
    auto asyncPReadContext = std::make_shared<Internal::AsyncCb::AsyncPReadContext>(buf, toRead);
    // each datanode needs a AsyncPRederV2.
    std::vector<std::shared_ptr<Internal::AsyncCb::AsyncPReaderV2>> readers;

    // each datanode will open a read session. The sessionContexts[i] is used to wait for the result of datanode[i]
    std::vector<AsyncCb::AsyncPReadSessionContext::Ptr> sessionContexts;

    // for each datanode, we create one reader and one corresponding AsyncPReadSessionContext
    // to wait the result of the read to that datanode.
    for (const DatanodeInfo & dn : sortedLocations)
    {
        readers.push_back(std::make_shared<Internal::AsyncCb::AsyncPReaderV2>(
            filesystem, curBlock, dn, *peerCache, offset, len, curBlock->getToken(), ASYNC_CLIENT_NAME, conf->getVerifyChecksum(), conf));
        sessionContexts.push_back(asyncPReadContext->createSession());
    }

    for (int i = 0; i < readers.size(); i++)
    {
        auto readContext = sessionContexts[i];
        // the function will be called when any error occurs.
        readers[i]->onError([readContext = sessionContexts[i]](boost::system::error_code ec) {
            std::unique_lock lk(*readContext->shared->mu);
            readContext->setEc(ec);
            readContext->setAborted();
            readContext->shared->notice();
        });

        // the function will be called when the read succeed.
        readers[i]->onSucc([readContext = sessionContexts[i]](boost::system::error_code ec) {
            std::unique_lock lk(*readContext->shared->mu);
            readContext->setEc(ec);
            readContext->setFinished();
            readContext->shared->notice();
        });
        readers[i]->setIndex(i);
    }

    asyncPReadContext->setProcessing();
    for (int i = 0; i < readers.size(); i++)
    {
        auto readContext = sessionContexts[i];
        auto reader = readers[i];
        int delay = i * conf->getHedgedReadIntervalMs();
        readContext->setProcessing();
        // fire tasks
        reader->setDelay(delay);
        reader->initReadTask(readContext);
    }

    //wait result.
    {
        std::unique_lock lk(*asyncPReadContext->mu);
        int wakeupCount = 0;
        auto wait_succ = asyncPReadContext->cond->wait_for(
            lk, std::chrono::milliseconds(conf->getHedgedReadMaxWaitTime()), [&asyncPReadContext, &sessionContexts, &wakeupCount, this, offset] {
                ++wakeupCount;
                int aborted = 0;
                for (int i = 0; i < sessionContexts.size(); i++)
                {
                    if (sessionContexts[i]->isAborted())
                    {
                        if (sessionContexts[i]->getEc() != hdfs_async_errors::operation_cancelled && aborted == 0)
                        {
                            block_location_manager_.removeBlockLocationCache(path);
                        }
                        aborted += 1;
                    }
                    if (sessionContexts[i]->isFinished())
                    {
                        asyncPReadContext->setFinished();
                        if (i != 0)
                        {
                            LOG(DEBUG3,
                                "[AsyncPReaderV2] tid %llu hedgeread %d succeeded, block: %s, file: %s, offset: %lld",
                                std::hash<std::thread::id>{}(std::this_thread::get_id()),
                                i,
                                curBlock->toString().c_str(),
                                path.c_str(),
                                offset);
                        }
                        return true;
                    }
                }
                if (aborted == sessionContexts.size())
                {
                    asyncPReadContext->setAborted();
                    return true;
                }
                return false;
            });
        // no request responded on time.
        if (!wait_succ)
        {
            std::stringstream ss;
            ss << "[AsyncPReaderV2] [wait timeout] all reads to block" << curBlock->toString() << " for file " << path << " timeout and "
               << (shouldUpdateMetaDataOnFailure ? "will retry" : "will not retry") << ", error codes ";
            for (int i = 0; i < sessionContexts.size(); i++)
            {
                ss << "[ read " << i << ":" << sessionContexts[i]->getEc().message() << "]";
            }
            ss << " wakeupCount = " << wakeupCount;
            LOG(WARNING, "%s", ss.str().c_str());
            asyncPReadContext->setAborted();
        }
    }


    if (asyncPReadContext->isTaskAborted())
    {
        block_location_manager_.removeBlockLocationCache(path);
        std::stringstream ss;
        ss << "[AsyncPReaderV2] all reads to block" << curBlock->toString() << " for file " << path << " failed and "
           << (shouldUpdateMetaDataOnFailure ? "will retry" : "will not retry") << ", error codes ";
        for (int i = 0; i < sessionContexts.size(); i++)
        {
            ss << "[ read " << i << ":" << sessionContexts[i]->getEc().message() << "]";
        }
        if (shouldUpdateMetaDataOnFailure)
        {
            LOG(WARNING, "%s", ss.str().c_str());
            return -1;
        }
        else
        {
            THROW(HdfsIOException, "%s", ss.str().c_str())
        }
    }
    return asyncPReadContext->nread;
}
int32_t InputStreamImpl::readOneBlock(char * buf, int32_t size, bool shouldUpdateMetadataOnFailure, bool pread)
{
    bool temporaryDisableLocalRead = false;
    std::string buffer;

    while (true)
    {
        const int32_t bytesRemaining = size - totalBytesRead;
        try
        {
            /*
             * Setup block reader here and handle failure.
             */
            if (!blockReader)
            {
                setupBlockReader(temporaryDisableLocalRead, bytesRemaining, pread);
                temporaryDisableLocalRead = false;
            }
        }
        catch (const HdfsInvalidBlockToken & e)
        {
            block_location_manager_.removeBlockLocationCache(path);
            LOG(LOG_ERROR,
                "InputStreamImpl: failed to read Block (stale token): %s file %s, \n%s, retry after updating block informations.",
                curBlock->toString().c_str(),
                path.c_str(),
                GetExceptionDetail(e, buffer));
            return -1;
        }
        catch (const HdfsIOException & e)
        {
            /*
             * In setupBlockReader, we have tried all the replicas.
             * We now update block informations once, and try again.
             */
            block_location_manager_.removeBlockLocationCache(path);
            if (shouldUpdateMetadataOnFailure)
            {
                if (conf->getEncryptedDatanode() || conf->getSecureDatanode())
                    LOG(WARNING,
                        "InputStreamImpl: failed to read Block: %s file %s, retry after updating block informations.",
                        curBlock->toString().c_str(),
                        path.c_str());
                else
                    LOG(LOG_ERROR,
                        "InputStreamImpl: failed to read Block: %s file %s, \n%s, retry after updating block informations.",
                        curBlock->toString().c_str(),
                        path.c_str(),
                        GetExceptionDetail(e, buffer));
                return -1;
            } else {
                /*
                 * We have updated block informations and failed again.
                 */
                throw;
            }
        }

        /*
         * Block reader has been setup, read from block reader.
         */
        bool hasFinishedReading = false;
        bool currentNodeSlow = false;
        try {
            int32_t toRead = (bytesRemaining < endOfCurBlock - cursor ?
                              bytesRemaining : static_cast<int32_t>(endOfCurBlock - cursor));
            /*
             * In case of pread, we reset block reader after read once,
             * so we try to read all needed data to reduce network reconnecting.
             */
            do // the do-while loop here seems useless since we have `readFully`.
            {
                assert(blockReader);

                auto todo = toRead;

//                // debug code.
//                constexpr auto throttleSpeedForSimulation = false;
//                if constexpr (throttleSpeedForSimulation) {
//                    todo = todo > 100 ? todo / 8 : todo;
//                }
                int32_t bytesRead = blockReader->read(buf + totalBytesRead, todo); // besides the original hdfs exception, the read will also throw HDFSIOException on slow read.
                // I think we should break.
                if (bytesRead <= 0)
                    break;

                totalBytesRead += bytesRead;
                cursor += bytesRead;
                toRead -= bytesRead;

                hasFinishedReading = toRead == 0;
            } while (pread && toRead > 0);

            // for pread we always reset the block reader
            // after we are done reading, or the node is reading slowly.
            if(pread) {
                blockReader.reset();
            }

            // if it's !pread, we terminate regardless.
            // if it's
            if (hasFinishedReading || !pread)
            {
                return totalBytesRead;
            }

            //if we reached here, the following are true
            // 1. we are pread
            // 2. we read some bytes, but we haven't finished reading
            // this indicates that we had a slow node, as such, we need to continue.
            // we continue because the code after this is purely for exception handling.
            continue;
        } catch (const HdfsSlowReadException & e) {
            LOG(WARNING,
                "[SLOW NODE] InputStreamImpl: failed to read Block: %s file %s from Datanode: %s, \n%s, "
                "retry read again from another Datanode.",
                curBlock->toString().c_str(), path.c_str(),
                curNode.formatAddress().c_str(), GetExceptionDetail(e, buffer));
            currentNodeSlow = true;

        } catch (const HdfsIOException & e) {
            /*
             * Failed to read from current block reader,
             * add the current datanode to invalid node list and try again.
             */
            LOG(LOG_ERROR,
                "InputStreamImpl: failed to read Block: %s file %s from Datanode: %s, \n%s, "
                "retry read again from another Datanode.",
                curBlock->toString().c_str(), path.c_str(),
                curNode.formatAddress().c_str(), GetExceptionDetail(e, buffer));
            if (conf->doesNotRetryAnotherNode()) {
                throw;
            }
        } catch (const ChecksumException & e) {
            LOG(LOG_ERROR,
                "InputStreamImpl: failed to read Block: %s file %s from Datanode: %s, \n%s, "
                "retry read again from another Datanode.",
                curBlock->toString().c_str(), path.c_str(),
                curNode.formatAddress().c_str(), GetExceptionDetail(e, buffer));
        }

        /*
         * Successfully create the block reader but failed to read.
         * Disable the local block reader and try the same node again.
         */
        if (!blockReader || dynamic_cast<LocalBlockReader *>(blockReader.get())) {
            temporaryDisableLocalRead = true;
        } else {
            /*
             * Remote block reader failed to read, try another node.
             */
            if(currentNodeSlow)
            {
                setSlowNode(curNode);
                LOG(INFO,
                    "[SLOW NODE] IntputStreamImpl: Add invalid datanode %s to slow datanodes and try another datanode again for file %s.",
                    curNode.formatAddress().c_str(),
                    path.c_str());
                CNCH_HDFS_CALLBACK1(hdfs_event_callback,Event::HdfsSlowNodeEvent);
            } else {
                setFailedNode(curNode);
                LOG(INFO,
                    "IntputStreamImpl: Add invalid datanode %s to failed datanodes and try another datanode again for file %s.",
                    curNode.formatAddress().c_str(),
                    path.c_str());
                CNCH_HDFS_CALLBACK1(hdfs_event_callback,Event::HdfsFailedNodeEvent);
                block_location_manager_.removeBlockLocationCache(path);
            }
        }

        blockReader.reset();
    }
}

/**
 * To read data from hdfs.
 * @param buf the buffer used to filled.
 * @param size buffer size.
 * @param pread is it pread.
 * @return return the number of bytes filled in the buffer, it may less than size.
 */
int32_t InputStreamImpl::readInternal(char * buf, int32_t size, bool pread) {
    int updateMetadataOnFailure = conf->getMaxReadBlockRetry();
    bool reset_parameter_for_fsr = true;
    totalBytesRead = 0 ;
    try {
        do {
            const LocatedBlock * lb = NULL;

            /*
             * Check if we have got the block information we need.
             */
            if (!lbs || cursor >= getFileLength()
                    || (cursor >= endOfCurBlock && !(lb = lbs->findBlock(cursor)))) {
                /*
                 * Get block information from namenode.
                 * Do RPC failover work in updateBlockInfos.
                 */
                updateBlockInfos();

                /*
                 * We already have the up-to-date block information,
                 * Check if we reach the end of file.
                 */
                if (cursor >= getFileLength()) {
                    THROW(HdfsEndOfStream,
                          "InputStreamImpl: read over EOF, current position: %" PRId64 ", read size: %d, from file: %s",
                          cursor, size, path.c_str());
                }
            }

            /*
             * If we reach the end of block or the block information has just updated,
             * seek to the right block to read.
             */
            if (cursor >= endOfCurBlock) {
                lb = lbs->findBlock(cursor);

                if (!lb) {
                    THROW(HdfsIOException,
                        "InputStreamImpl: cannot find block information at position: %" PRId64 " for file: %s",
                        cursor,
                        path.c_str());
                }

                /*
                 * Seek to the right block, setup all needed variable,
                 * but do not setup block reader, setup it latter.
                 */
                seekToBlock(*lb);
            }

            int retval = 0;
            if (!enableHedgedRead)
            {
                // readOneBlock will modify cursor, so it is not idempotent.
                retval = readOneBlock(buf, size, updateMetadataOnFailure > 0, pread);
                if (switch_read_strategy_)
                {
                    switch_read_strategy_->resetFactor(reset_parameter_for_fsr);
                }
            }
            else
            {
                // use hedge read.
                retval = readOneBlockAsyncPReadV2(buf, size, updateMetadataOnFailure > 0);
                if (retval >= 0)
                {
                    cursor += retval; // modify the cursor since we didn't update it in read as before.
                }
            }

            /*
             * Now we have tried all replicas and failed.
             * We will update metadata once and try again.
             */
            if (retval < 0)
            {
                lbs.reset();
                reset_parameter_for_fsr = false;
                endOfCurBlock = 0;
                --updateMetadataOnFailure;
                try
                {
                    sleep_for(seconds(1));
                }
                catch (...)
                {
                }

                continue;
            }
            return retval;
        } while (true);
    } catch (const HdfsCanceled & e) {
        throw;
    } catch (const HdfsEndOfStream & e) {
        throw;
    } catch (const HdfsException & e) {
        /*
         * wrap the underlying error and rethrow.
         */
        NESTED_THROW(HdfsIOException,
                     "InputStreamImpl: cannot read file: %s, from position %" PRId64 ", size: %d.",
                     path.c_str(), cursor, size);
    }
}

/**
 * To read data from hdfs, block until get the given size of bytes.
 * @param buf the buffer used to filled.
 * @param size the number of bytes to be read.
 */
void InputStreamImpl::readFully(char * buf, int64_t size) {
    LOG(DEBUG3, "readFully file %s size is %" PRId64 ", offset %" PRId64, path.c_str(), size, cursor);
    checkStatus();
    try {
        return readFullyInternal(buf, size);
    } catch (const HdfsEndOfStream & e) {
        throw;
    } catch (...) {
        lastError = current_exception();
        throw;
    }
}

void InputStreamImpl::readFullyInternal(char * buf, int64_t size) {
    int32_t done;
    int64_t pos = cursor, todo = size;

    try {
        while (todo > 0) {
            done = todo < std::numeric_limits<int32_t>::max() ?
                   static_cast<int32_t>(todo) :
                   std::numeric_limits<int32_t>::max();
            done = readInternal(buf + (size - todo), done, false);
            todo -= done;
        }
    } catch (const HdfsCanceled & e) {
        throw;
    } catch (const HdfsEndOfStream & e) {
        THROW(HdfsEndOfStream,
              "InputStreamImpl: read over EOF, current position: %" PRId64 ", read size: %" PRId64 ", from file: %s",
              pos, size, path.c_str());
    } catch (const HdfsException & e) {
        NESTED_THROW(HdfsIOException,
                     "InputStreamImpl: cannot read fully from file: %s, from position %" PRId64 ", size: %" PRId64 ".",
                     path.c_str(), pos, size);
    }
}

int64_t InputStreamImpl::available() {
    checkStatus();

    try {
        if (blockReader) {
            return blockReader->available();
        }
    } catch (...) {
        lastError = current_exception();
        throw;
    }

    return 0;
}

/**
 * To move the file point to the given position.
 * @param size the given position.
 */
void InputStreamImpl::seek(int64_t pos) {
    LOG(DEBUG2, "%p seek file %s to %" PRId64 ", offset %" PRId64, this, path.c_str(), pos, cursor);
    checkStatus();

    try {
        seekInternal(pos);
    } catch (...) {
        lastError = current_exception();
        throw;
    }
}

void InputStreamImpl::seekInternal(int64_t pos) {
    if (cursor == pos) {
        return;
    }

    if (!lbs || pos > getFileLength()) {
        updateBlockInfos();

        if (pos > getFileLength()) {
            THROW(HdfsEndOfStream,
                  "InputStreamImpl: seek over EOF, current position: %" PRId64 ", seek target: %" PRId64 ", in file: %s",
                  cursor, pos, path.c_str());
        }
    }

    try {
        if (blockReader && pos > cursor && pos < endOfCurBlock) {
            blockReader->skip(pos - cursor);
            cursor = pos;
            return;
        }
    } catch (const HdfsIOException & e) {
        std::string buffer;
        LOG(LOG_ERROR, "InputStreamImpl: failed to skip %" PRId64 " bytes in current block reader for file %s\n%s",
            pos - cursor, path.c_str(), GetExceptionDetail(e, buffer));
        LOG(INFO, "InputStreamImpl: retry to seek to position %" PRId64 " for file %s", pos, path.c_str());
    } catch (const ChecksumException & e) {
        std::string buffer;
        LOG(LOG_ERROR, "InputStreamImpl: failed to skip %" PRId64 " bytes in current block reader for file %s\n%s",
            pos - cursor, path.c_str(), GetExceptionDetail(e, buffer));
        LOG(INFO, "InputStreamImpl: retry to seek to position %" PRId64 " for file %s", pos, path.c_str());
    }

    /**
     * the seek target exceed the current block or skip failed in current block reader.
     * reset current block reader and set the cursor to the target position to seek.
     */
    endOfCurBlock = 0;
    blockReader.reset();
    cursor = pos;
}

/**
 * To get the current file point position.
 * @return the position of current file point.
 */
int64_t InputStreamImpl::tell()
{
    checkStatus();
    LOG(DEBUG2, "tell file %s at %" PRId64, path.c_str(), cursor);
    return cursor;
}

/**
 * Close the stream.
 */
void InputStreamImpl::close() {
}
void InputStreamImpl::closeShared()
{
    closed = true;
    localRead = true;
    readFromUnderConstructedBlock = false;
    verify = true;
    cursor = 0;
    endOfCurBlock = 0;
    lastBlockBeingWrittenLength = 0;
    totalBytesRead = 0;
    prefetchSize = 0;
    blockReader.reset();
    curBlock.reset();
    lbs.reset();
    conf.reset();
    failedNodes.clear();
    slowNodes.clear();
    path.clear();
    localReaderBuffer.resize(0);
    lastError = exception_ptr();
}

std::string InputStreamImpl::toString() {
    if (path.empty()) {
        return std::string("InputStream for path ") + path;
    } else {
        return std::string("InputStream (not opened)");
    }
}

}
}
