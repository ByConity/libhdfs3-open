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
// Created by Renming Qi on 22/3/22.
//
#include <boost/asio/io_context.hpp>
#include <sys/prctl.h>

#include "AsioGlobalContext.h"

namespace Hdfs
{
namespace Internal
{
    namespace AsyncCb
    {
        static boost::asio::io_context io_context;
        AsioGlobalContext & AsioGlobalContext::Instance()
        {
            static AsioGlobalContext asio_global_context;
            return asio_global_context;
        }
        boost::asio::io_context & AsioGlobalContext::getIOContext()
        {
            return io_context;
        }

        LruMultiMap<std::string, AsioGlobalContext::AsyncSocketWithTTL> & AsioGlobalContext::getAsyncSocketMap()
        {
            return async_socket_map;
        }

        AsioGlobalContext::AsioGlobalContext() : io_context() , async_socket_map()
        {
            //            auto count = std::thread::hardware_concurrency() / 8;
            auto count = std::thread::hardware_concurrency();
            for (int i = 0; i < count; i++)
            {
                // the work guard here is used to keep run() away from returning when there is no work.
                threads.emplace_back([&] {
                    std::string threadName = "hedge-read-" + std::to_string(i);
                    prctl(PR_SET_NAME, threadName.c_str(), 0, 0, 0);
                    auto work = boost::asio::require(io_context.get_executor(), boost::asio::execution::outstanding_work.tracked);
                    io_context.run();
                });
            }
        }
        AsioGlobalContext::~AsioGlobalContext()
        {
            async_socket_map.clear();
            io_context.stop();
            for (auto & th : threads)
            {
                if (th.joinable())
                {
                    th.join();
                }
            }
        }

        static AsioGlobalContext globalContextInitializer;
    }


}
}
