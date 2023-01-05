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
