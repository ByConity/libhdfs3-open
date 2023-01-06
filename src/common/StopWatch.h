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

#ifndef _HDFS_LIBHDFS3_COMMON_STOPWATCH_H_
#define _HDFS_LIBHDFS3_COMMON_STOPWATCH_H_

#include <time.h>

namespace Hdfs::Internal
{

namespace StopWatchDetail
{
    inline uint64_t nanoseconds(clockid_t clock_type)
    {
        struct timespec ts;
        clock_gettime(clock_type, &ts);
        return uint64_t(ts.tv_sec * 1000000000LL + ts.tv_nsec);
    }
}


/** Differs from Poco::Stopwatch only by using 'clock_gettime' instead of 'gettimeofday',
  *  returns nanoseconds instead of microseconds, and also by other minor differencies.
  */
class Stopwatch
{
public:
    /** CLOCK_MONOTONIC works relatively efficient (~15 million calls/sec) and doesn't lead to syscall.
      * Pass CLOCK_MONOTONIC_COARSE, if you need better performance with acceptable cost of several milliseconds of inaccuracy.
      */
    Stopwatch(clockid_t clock_type_ = CLOCK_MONOTONIC) : clock_type(clock_type_) { start(); }

    void start()                       { start_ns = nanoseconds(); is_running = true; }
    void stop()                        { stop_ns = nanoseconds(); is_running = false; }
    void reset()                       { start_ns = 0; stop_ns = 0; is_running = false; }
    void restart()                     { start(); }
    uint64_t elapsed() const             { return elapsedNanoseconds(); }
    uint64_t elapsedNanoseconds() const  { return is_running ? nanoseconds() - start_ns : stop_ns - start_ns; }
    uint64_t elapsedMicroseconds() const { return elapsedNanoseconds() / 1000U; }
    uint64_t elapsedMilliseconds() const { return elapsedNanoseconds() / 1000000UL; }
    double elapsedSeconds() const      { return static_cast<double>(elapsedNanoseconds()) / 1000000000ULL; }

private:
    uint64_t start_ns = 0;
    uint64_t stop_ns = 0;
    clockid_t clock_type;
    bool is_running = false;

    uint64_t nanoseconds() const { return StopWatchDetail::nanoseconds(clock_type); }
};

}

#endif
