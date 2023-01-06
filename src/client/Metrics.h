
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