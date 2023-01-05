//
// Created by Renming Qi on 18/3/22.
//
#ifndef CLICKHOUSE_AsyncPReaderV2CALLBACK_H
#define CLICKHOUSE_AsyncPReaderV2CALLBACK_H
#include <memory>
#include <tuple>
#include <boost/asio.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/spawn.hpp>
#include <boost/beast.hpp>

#include <google/protobuf/io/coded_stream.h>

#include <condition_variable>
#include <mutex>
#include "AsyncPReaderError.h"
#include "BigEndian.h"
#include "Exception.h"
#include "ExceptionInternal.h"
#include "HWCrc32c.h"
#include "SWCrc32c.h"
#include "WriteBuffer.h"
#include "client/DataTransferProtocolSender.h"
#include "client/HdfsEvent.h"
#include "client/Metrics.h"
#include "client/RemoteBlockReader.h"
#include "server/LocatedBlock.h"
namespace Hdfs
{
namespace Internal
{
    namespace AsyncCb
    {
        using namespace boost::asio::ip;
        using boost::asio::ip::tcp;
        using boost::system::error_code;
        namespace bnet = boost::beast::net;
        using boost::beast::flat_buffer;


        enum AsyncPReadSessionStatus
        {
            Init = 0, // task is inited. (default)
            Processing, // task is still in processing
            Finished, // task is finished, that is, the read is done.
            Aborted // task is aborted due to cancellation or some exceptions.
        };

        /* AsyncPreadContext is used to wait the async read operation to be finished.
         * Each AsyncPreadContext will spawn several AsyncPreadSessionContexts
         * (the spawned number equals to the number of  datanodes).
         * Each AsyncPreadSessionContexts represents a read session to the corresponding datanode.
         * AsyncPreadContext will stop waiting if one of the session context finished or all of them are aborted.
        */
        struct AsyncPReadSessionContext;
        struct AsyncPReadContext : public std::enable_shared_from_this<AsyncPReadContext>
        {
            using Ptr = std::shared_ptr<AsyncPReadContext>;
            using CondPtr = std::shared_ptr<std::condition_variable>;
            using MutexPtr = std::shared_ptr<std::mutex>;

            AsyncPReadContext(char * buf_, size_t readSize_);
            std::shared_ptr<AsyncPReadSessionContext> createSession();
            size_t readSize;
            char * buf;
            MutexPtr mu;
            CondPtr cond;

            size_t nread = 0;
            std::atomic<AsyncPReadSessionStatus> taskStatus{AsyncPReadSessionStatus::Init};

            void copyBuffer(int dstOffset, const char * src, int len);

            void setAborted() { taskStatus.store(AsyncPReadSessionStatus::Aborted); }
            bool isTaskAborted() { return taskStatus.load() == AsyncPReadSessionStatus::Aborted; }
            void setFinished() { taskStatus.store(AsyncPReadSessionStatus::Finished); }
            bool isTaskFinished() { return taskStatus.load() == AsyncPReadSessionStatus::Finished; }

            void setProcessing() { taskStatus.store(AsyncPReadSessionStatus::Processing); }
            bool isTaskInProcessing() { return taskStatus.load() == AsyncPReadSessionStatus::Processing; }

            void notice() const { cond->notify_one(); }
        };

        struct AsyncPReadSessionContext
        {
            using Ptr = std::shared_ptr<AsyncPReadSessionContext>;
            AsyncPReadContext::Ptr shared;
            std::atomic<AsyncPReadSessionStatus> sessionStatus{AsyncPReadSessionStatus::Init};
            error_code ec = hdfs_async_errors::undetermined;

            void setEc(error_code ec_) { ec = ec_; }

            error_code getEc() { return ec; }
            void setAborted() { sessionStatus.store(AsyncPReadSessionStatus::Aborted); }
            bool isAborted() { return sessionStatus.load() == AsyncPReadSessionStatus::Aborted; }
            void setFinished() { sessionStatus.store(AsyncPReadSessionStatus::Finished); }
            bool isFinished() { return sessionStatus.load() == AsyncPReadSessionStatus::Finished; }
            void setProcessing() { sessionStatus.store(AsyncPReadSessionStatus::Processing); }
            bool isReadNeedProcessing() { return sessionStatus.load() == AsyncPReadSessionStatus::Processing; }
        };

        /*
         * AsioGlobalContext is the io_context of boost asio, which is responsible for scheduling and executing async tasks.
         */
        class AsioGlobalContext
        {
        public:
            AsioGlobalContext();
            ~AsioGlobalContext();
            std::vector<std::thread> threads;
            static boost::asio::io_context & Instance();
        };
        /*
         * This class is used to read data from one datanode in an async way using boost asio's ability.
         * Simply speaking, in asio, our program uses I/O objects (e.g., socket, async timer). The request to I/O object
         * is then forward to I/O execution context (AsioGlobalContext::Instance() in our case)  which will then signal the OS to start async
         * operations. Once the async operation is ready, the OS indicate the operation's completion by placing the result on a queue, which is ready
         * to be picked by the I/O execution context.
         * The I/O execution context deqeues the result and call our completion handler (the callback).
         * For more detailed explanation, check
         * https://www.boost.org/doc/libs/1_79_0/doc/html/boost_asio/overview/basics.html
         * There are two unmentioned points that you may concern:
         * 1. the operations are stored in a queue (which is basically a linked list), so there's no upper limited of pending async tasks.
         * 2. the asio using epoll behind the socket I/O object, so the operations on socket will not block the executing thread.
         */
        class AsyncPReaderV2 : public std::enable_shared_from_this<AsyncPReaderV2>
        {
        public:
            using ConnectionPtr = std::shared_ptr<tcp::socket>;
            using TcpStreamPtr = std::shared_ptr<boost::beast::tcp_stream>;
            AsyncPReaderV2(
                std::shared_ptr<FileSystemInter> filesystem,
                std::shared_ptr<LocatedBlock> eb,
                const DatanodeInfo & datanode,
                PeerCache & peerCache,
                int64_t start,
                int64_t len,
                const Token & token,
                const std::string & clientName,
                bool verify,
                std::shared_ptr<SessionConfig> conf);

            ~AsyncPReaderV2();

            void initReadTask(AsyncPReadSessionContext::Ptr ctx);

            void initReadTaskNoDelay();

            void handleSendReadOp();

            void handleReceiveReadOp(error_code ec, size_t length);

            void handleReceiveReadOpProto(uint32_t resp_size, error_code ec, size_t length);

            void handleError(error_code ec);

            void handleSucc();

            void handleReadPacketHeader(error_code ec, size_t length, bool isTrailing = false);

            void handleReadPacketBody(int dataSize, error_code ec, size_t length);

            void handleSendStatus();

            void onError(std::function<void(error_code)> cb) { errorCallback = cb; }

            void setDelay(int64_t mill);

            void setIndex(int index_ = 0);

            void onSucc(std::function<void(error_code)> cb) { succCallback = cb; }


        private:
            error_code verifyChecksum(int chunks);

            bool sentStatus;
            bool verify; //verify checksum or not.
            shared_ptr<LocatedBlock> binfo;
            DatanodeInfo datanode;
            int checksumSize;
            int chunkSize;
            int connTimeout;
            int position; //point in buffer.
            int readTimeout;
            int size; //data size in buffer.
            int writeTimeout;
            int64_t start;
            int64_t len;
            int64_t cursor; //point in block.
            int64_t endOffset; //offset in block requested to read to.
            Token token;
            int64_t lastSeqNo; //segno of the last chunk received
            PeerCache & peerCache;
            shared_ptr<Checksum> checksum;
            shared_ptr<PacketHeader> lastHeader;
            std::vector<char> buffer;
            shared_ptr<FileSystemInter> filesystem;
            TcpStreamPtr tcpStream;
            std::string clientName;
            shared_ptr<SessionConfig> conf;
//            int64_t bodyBufferSize;
            flat_buffer bodyBuffer; // the buffer for storing tcp data read from socket.
            // check https://www.boost.org/doc/libs/1_79_0/libs/beast/doc/html/beast/ref/boost__beast__flat_buffer.html for usages.

            AsyncPReadSessionContext::Ptr readContext = nullptr;
            int readBytes = 0;

            std::function<void(error_code)> errorCallback = nullptr;
            std::function<void(error_code)> succCallback = nullptr;

            uint64_t delayMills = 0;

            int index = 0;

            std::unique_ptr<boost::asio::steady_timer> delayTimer = nullptr;

            int packetCount = 0;
        };
    }
}
}
#endif