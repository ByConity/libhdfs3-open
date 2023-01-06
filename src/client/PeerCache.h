
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
#ifndef _HDFS_LIBHDFS3_CLIENT_PEERCACHE_H_
#define _HDFS_LIBHDFS3_CLIENT_PEERCACHE_H_

#include <string>
#include <utility>

#include <boost/beast.hpp>
#include "common/DateTime.h"
#include "common/LruMap.h"
#include "common/Memory.h"
#include "common/SessionConfig.h"
#include "network/Socket.h"
#include "server/DatanodeInfo.h"
namespace Hdfs
{
namespace Internal
{

    class PeerCache
    {
    public:
        explicit PeerCache(const SessionConfig & conf);

        //  template<typename SocketType>
        //  shared_ptr<SocketType> getConnection(const DatanodeInfo& datanode);
        //
        //  template<typename SocketType>
        //  void addConnection(shared_ptr<SocketType> peer, const DatanodeInfo& datanode);

        template <typename SocketType>
        shared_ptr<SocketType> getConnection(const DatanodeInfo & datanode)
        {
            std::string key = buildKey(datanode);
            std::pair<shared_ptr<SocketType>, steady_clock::time_point> value;
            int64_t elipsed;

            if (!getMap<SocketType>().findAndErase(key, &value))
            {
                //        LOG(DEBUG2, "PeerCache miss for datanode %s uuid(%s).",
                //            datanode.formatAddress().c_str(), datanode.getDatanodeId().c_str());
                return shared_ptr<SocketType>();
            }
            else if ((elipsed = ToMilliSeconds(value.second, steady_clock::now())) > expireTimeInterval)
            {
                //        LOG(DEBUG2, "PeerCache expire for datanode %s uuid(%s).",
                //            datanode.formatAddress().c_str(), datanode.getDatanodeId().c_str());
                return shared_ptr<SocketType>();
            }

            //    LOG(DEBUG2, "PeerCache hit for datanode %s uuid(%s), elipsed %" PRId64,
            //        datanode.formatAddress().c_str(), datanode.getDatanodeId().c_str(),
            //        elipsed);
            return value.first;
        }

        template <typename SocketType>
        void addConnection(shared_ptr<SocketType> peer, const DatanodeInfo & datanode)
        {
            std::string key = buildKey(datanode);
            std::pair<shared_ptr<SocketType>, steady_clock::time_point> value(peer, steady_clock::now());
            getMap<SocketType>().insert(key, value);
            //  LOG(DEBUG2, "PeerCache add for datanode %s uuid(%s).",
            //      datanode.formatAddress().c_str(), datanode.getDatanodeId().c_str());
        }
        typedef std::pair<shared_ptr<Socket>, steady_clock::time_point> value_type;
        typedef std::pair<shared_ptr<boost::beast::tcp_stream>, steady_clock::time_point> value_type_async;

    private:
        std::string buildKey(const DatanodeInfo & datanode);

        template <typename SocketType>
        static LruMultiMap<std::string, std::pair<shared_ptr<SocketType>, steady_clock::time_point>> & getMap()
        {
            if constexpr (std::is_same<boost::beast::tcp_stream, SocketType>::value)
            {
                return MapAsync;
            }
            else
            {
                return Map;
            }
        }

    private:
        const int cacheSize;
        int64_t expireTimeInterval; // milliseconds
        static LruMultiMap<std::string, value_type> Map; // this map is storing normal linux socket.
        static LruMultiMap<std::string, value_type_async>
            MapAsync; // this map is storing boost::asio::tcp_stream, which is a boost wrapper over socket.
    };
}
}

#endif /* _HDFS_LIBHDFS3_CLIENT_PEERCACHE_H_ */
