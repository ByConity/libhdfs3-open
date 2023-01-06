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
// Created by Renming Qi on 25/3/22.
//
#include "AsyncPReaderError.h"
namespace Hdfs
{
namespace Internal
{
    const char * HdsfAyncCategory::name() const BOOST_NOEXCEPT { return "hdfs.async"; }
    std::string HdsfAyncCategory::message(int ev) const
    {
        switch (ev)
        {
            case operation_cancelled:
                return "async operation is cancelled";
            case invalid_parse:
                return "protocol parsing failed";
            case invalid_checksum:
                return "invalid checksum";
            case invalid_operation:
                return "invalid checksum (might be a bug)";
            case invalid_token:
                return "invalid datanode token";
            case invalid_response:
                return "invalid response from dn";
            case rpc_failed:
                return "failed rpc to dn (check dn log)";
            case invalid_data:
                return "invalid data (might be a bug)";
            case undetermined: // dummy value used for uninitalized error code.
                return "value is not set";
        }
        return "unknown hdfs_async error, code: " + std::to_string(ev);
    }
    const HdsfAyncCategory & get_hdfs_async_category()
    {
        static HdsfAyncCategory instance;
        return instance;
    }
}
}
