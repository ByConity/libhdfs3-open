
/*
 * Copyright (2022) ByteDance Ltd.
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
#include <sys/prctl.h>

#include "client/PeerCache.h"
#include "client/async_preader/AsyncPReaderCallback.h"

/* ugly hack.
 * The rationale behind this is that we want the PeerCache::MapAsync which contains boost::asio::tcp_stream to be deconstructed
 * before the boost::asio::io_context, otherwise it will coredump.
 */

namespace Hdfs
{
namespace Internal
{
    namespace AsyncCb
    {
        boost::asio::io_context & AsioGlobalContext::Instance()
        {
            static boost::asio::io_context io_context;
            return io_context;
        }

        AsioGlobalContext::AsioGlobalContext()
        {
//            auto count = std::thread::hardware_concurrency() / 8;
            auto count =  std::thread::hardware_concurrency() ;
            for (int i = 0; i < count; i++)
            {
                // the work guard here is used to keep run() away from returning when there is no work.
                threads.emplace_back([&] {
                    std::string threadName = "hedge-read-" + std::to_string(i);
                    prctl(PR_SET_NAME,threadName.c_str(),0,0,0);
                    auto work = boost::asio::require(Instance().get_executor(), boost::asio::execution::outstanding_work.tracked);
                    Instance().run();
                });
            }
        }
        AsioGlobalContext::~AsioGlobalContext()
        {
            Instance().stop();
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


    LruMultiMap<std::string, PeerCache::value_type> PeerCache::Map;
    LruMultiMap<std::string, PeerCache::value_type_async> PeerCache::MapAsync;
}
}
