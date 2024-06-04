#ifndef CLICKHOUSE_ASIO_GLOBAL_CONTEXT_H
#define CLICKHOUSE_ASIO_GLOBAL_CONTEXT_H
#include <memory>
#include <boost/asio.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/spawn.hpp>
#include <boost/beast.hpp>
#include <common/LruMap.h>

namespace Hdfs
{
namespace Internal
{
    namespace AsyncCb
    {
        /*
         * AsioGlobalContext is the io_context of boost asio, which is responsible for scheduling and executing async tasks.
         */
        class AsioGlobalContext
        {
        public:
            using AsyncSocketWithTTL = std::pair<std::shared_ptr<boost::beast::tcp_stream>, std::chrono::steady_clock::time_point>;
            AsioGlobalContext();
            ~AsioGlobalContext();
            static AsioGlobalContext & Instance();
            boost::asio::io_context & getIOContext();
            LruMultiMap<std::string, AsyncSocketWithTTL> & getAsyncSocketMap();

        private:
            boost::asio::io_context io_context;
            LruMultiMap<std::string, AsyncSocketWithTTL> async_socket_map;
            std::vector<std::thread> threads;
        };
    }
}
}
#endif