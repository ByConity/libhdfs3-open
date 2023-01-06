#ifndef _HDFS_LIBHDFS3_HDFS_EVENT_H_
#define _HDFS_LIBHDFS3_HDFS_EVENT_H_
#include <stdint.h>
namespace Hdfs
{
namespace Event
{
    constexpr int HDFS_EVENT_DUMMY = __COUNTER__;
    constexpr int HDFS_EVENT_SLOWNODE = __COUNTER__;
    constexpr int HDFS_EVENT_FAILEDNODE = __COUNTER__;
    constexpr int HDFS_EVENT_GET_BLOCK_LOCATION = __COUNTER__;
    constexpr int HDFS_EVENT_CREATE_BLOCK_READER = __COUNTER__;
    constexpr int HDFS_EVENT_READ_PACKET = __COUNTER__;
//    constexpr int HDFS_EVENT_READ_PACKET_MAX = __COUNTER__;
    constexpr int HDFS_EVENT_DN_CONNECTION = __COUNTER__;
//    constexpr int HDFS_EVENT_DN_CONNECTION_MAX = __COUNTER__;
struct HdfsEvent
    {
        HdfsEvent(int type_, int64_t value_)
        {
            eventType = type_;
            value = value_;
        }
        int eventType = 0;
        int64_t value = 0;
    };
    typedef void (*EventCallBack)(const HdfsEvent &);

    const HdfsEvent HdfsSlowNodeEvent(HDFS_EVENT_SLOWNODE, 1);
    const HdfsEvent HdfsFailedNodeEvent(HDFS_EVENT_FAILEDNODE, 1);
}
}
using hdfsEventCallBack = Hdfs::Event::EventCallBack;
using hdfsEvent = Hdfs::Event::HdfsEvent;

#define CNCH_HDFS_CALLBACK1(cb, v) \
    { \
        if (cb != nullptr) \
            cb(v); \
    }
#define CNCH_HDFS_CALLBACK2(cb, t, v) \
    { \
        if (cb != nullptr) \
            cb({t, v}); \
    }

#endif /* (_HDFS_LIBHDFS3_HDFS_EVENT_H_) */
