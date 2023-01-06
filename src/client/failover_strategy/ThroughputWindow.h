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

#ifndef HDFS_CLIENT_THROUGHPUTWINDOW_H
#define HDFS_CLIENT_THROUGHPUTWINDOW_H

#include <chrono>
#include <deque>
#include <vector>
#define SEC_TO_NANOSEC_CONSTANT 1000000000

namespace Hdfs
{
namespace Internal
{
    struct Throughput
    {
        Throughput(
            int64_t bytes, std::chrono::nanoseconds latency_ns, std::chrono::time_point<std::chrono::steady_clock> io_finish_timestamp)
            : io_bytes_(bytes)
            , io_start_timestamp_(io_finish_timestamp - latency_ns)
            , io_finish_timestamp_(io_finish_timestamp)
            , io_cost_time_ns_(latency_ns)
        {
        }

        int64_t io_bytes_;
        std::chrono::time_point<std::chrono::steady_clock> io_start_timestamp_;
        std::chrono::time_point<std::chrono::steady_clock> io_finish_timestamp_;
        std::chrono::nanoseconds io_cost_time_ns_;
    };

    class ThroughputWindow
    {
    public:
        ThroughputWindow(std::chrono::nanoseconds time_window_length_ns, std::chrono::nanoseconds minimum_time_required_ns)
            : time_window_length_ns_(time_window_length_ns), minimum_time_required_ns_(minimum_time_required_ns)
        {
        }

        int64_t calculateThroughputInWindow(
            int64_t num_bytes, std::chrono::nanoseconds latency_ns, std::chrono::time_point<std::chrono::steady_clock> io_finish_timestamp)
        {
            total_io_bytes_in_window_ += num_bytes;
            total_io_time_ += latency_ns;

            std::chrono::time_point<std::chrono::steady_clock> time_lower_boundary = io_finish_timestamp - time_window_length_ns_;
            auto iter = io_records_.begin();
            while (iter != io_records_.end())
            {
                if (iter->io_start_timestamp_ < time_lower_boundary)
                {
                    total_io_bytes_in_window_ -= iter->io_bytes_;
                    total_io_time_ -= iter->io_cost_time_ns_;
                    iter = io_records_.erase(iter);
                }
                else
                {
                    break;
                }
            }

            io_records_.emplace_back(num_bytes, latency_ns, io_finish_timestamp);

            if (total_io_time_ < minimum_time_required_ns_)
            {
                return INT64_MAX;
            }

            return total_io_bytes_in_window_ * SEC_TO_NANOSEC_CONSTANT / total_io_time_.count();
        }

    private:
        std::chrono::nanoseconds time_window_length_ns_;
        std::chrono::nanoseconds minimum_time_required_ns_;
        int64_t total_io_bytes_in_window_{0};
        std::chrono::nanoseconds total_io_time_{0};

        std::deque<Throughput> io_records_;
    };

} // namespace Internal
} // namespace Hdfs

#endif // HDFS_CLIENT_THROUGHPUTWINDOW_H
