#ifndef _HDFS_LIBHDFS3_CLIENT_METRICS_H_
#define _HDFS_LIBHDFS3_CLIENT_METRICS_H_

#include "XmlConfig.h"
#include "ScopeGuard.h"
#include "StopWatch.h"
#include "Logger.h"


#define LOG_TIME(msg) \
    Hdfs::Internal::Stopwatch log_op_time_macro_stopwatch; \
    log_op_time_macro_stopwatch.start(); \
    SCOPE_EXIT({ \
        log_op_time_macro_stopwatch.stop(); \
        LOG(Hdfs::Internal::DEBUG2, "Hdfs opertaion %s takes %llu ms", msg, log_op_time_macro_stopwatch.elapsedMilliseconds()); \
    })

#define LOG_TIME_LV(level, msg) \
    Hdfs::Internal::Stopwatch log_op_time_macro_stopwatch; \
    log_op_time_macro_stopwatch.start(); \
    SCOPE_EXIT({ \
        log_op_time_macro_stopwatch.stop(); \
        LOG(level, "Hdfs opertaion %s takes %llu ms", msg, log_op_time_macro_stopwatch.elapsedMilliseconds()); \
    })

#endif /* _HDFS_LIBHDFS3_CLIENT_METRICS_H_ */