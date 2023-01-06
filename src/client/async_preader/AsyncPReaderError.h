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

#ifndef CLICKHOUSE_ASYNCPREADERERROR_H
#define CLICKHOUSE_ASYNCPREADERERROR_H
#include <boost/system/error_code.hpp>
namespace Hdfs
{
namespace Internal
{
    /*
     * define error codes for AsyncPReaderCallaback
     */
    enum hdfs_async_errors
    {
        operation_cancelled = 1,
        invalid_parse,
        invalid_checksum,
        invalid_operation,
        invalid_token,
        invalid_response,
        rpc_failed,
        invalid_data,
        undetermined
    };
    class HdsfAyncCategory : public boost::system::error_category
    {
    public:
        const char * name() const BOOST_NOEXCEPT;
        std::string message(int ev) const;
    };

    const HdsfAyncCategory & get_hdfs_async_category();
}
}
namespace boost
{
namespace system
{
    template <>
    struct is_error_code_enum<Hdfs::Internal::hdfs_async_errors>
    {
        static const bool value = true;
    };
}
}
namespace Hdfs
{
namespace Internal
{
    inline boost::system::error_code make_error_code(hdfs_async_errors e)
    {
        return boost::system::error_code(static_cast<int>(e), get_hdfs_async_category());
    }

}
}

#endif //CLICKHOUSE_ASYNCPREADERERROR_H
