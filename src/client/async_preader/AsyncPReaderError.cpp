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