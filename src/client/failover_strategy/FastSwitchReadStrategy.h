
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

#ifndef HDFS_CLIENT_FAILOVERSTRATEGY_FASTSWITCHREAD_H
#define HDFS_CLIENT_FAILOVERSTRATEGY_FASTSWITCHREAD_H

#include <chrono>
#include <memory>

#include <random>
#include "common/Exception.h"
#include "common/ExceptionInternal.h"
#include "common/SessionConfig.h"
#include "Logger.h"
#include "ScopeGuard.h"
#include "client/failover_strategy/ThroughputWindow.h"
#include "client/Metrics.h"
namespace Hdfs
{
namespace Internal
{
    class FastSwitchReadStrategy
    {
    public:
        FastSwitchReadStrategy(
            int32_t slow_io_threshold,
            int32_t read_io_timeout_ms,
            std::chrono::nanoseconds time_window_length_ns,
            std::chrono::nanoseconds minimum_time_required_ns,
            std::shared_ptr<SessionConfig> sess_conf)
            : slow_io_threshold_(slow_io_threshold)
            , read_io_timeout_ms_(read_io_timeout_ms)
            , time_window_length_ns_(time_window_length_ns)
            , minimum_time_required_ns_(minimum_time_required_ns)
            , sess_conf_(sess_conf)
        {
            window_ = std::make_shared<ThroughputWindow>(time_window_length_ns, minimum_time_required_ns);
        }

        void recordThroughput(
            int32_t ioBytes,
            std::chrono::time_point<std::chrono::steady_clock> io_start_time,
            std::chrono::time_point<std::chrono::steady_clock> io_end_time)
        {
            if (sess_conf_->enableFastSwitchRandomDelay)
            {
                int64_t random_delay = 0;
                if (random() % 10 >= 6)
                {
                    static std::default_random_engine generator;
                    std::uniform_int_distribution<uint64_t> distribution(
                        sess_conf_->getFsrRandomDelayLowerBound(), sess_conf_->getFsrRandomDelayUpperBound());
                    auto dice = std::bind(distribution, generator);
                    random_delay = dice();
                }
                std::chrono::milliseconds random_delay_ms(random_delay);
                io_end_time = io_start_time + random_delay_ms;
            }
            int64_t throughput = window_->calculateThroughputInWindow(ioBytes, io_end_time - io_start_time, io_end_time);
            if (throughput < slow_io_threshold_ / parameter_factor_)
            {
                LOG(WARNING,
                    "[SLOW NODE] Detect slow read. Try to failover to next node. throughput/threshold =  %lld / %lld ",
                    throughput,
                    slow_io_threshold_ / parameter_factor_);
                THROW(Hdfs::HdfsSlowReadException, "[SLOW NODE] Detect slow read. Try to failover to next node.");
            }
        }

        void resetFactor(bool reset_parameter)
        {
            if (reset_parameter)
            {
                // This is called when switch a new block
                parameter_factor_ = 1;
            }
            else
            {
                // This is called when switch the same block. Because all dn throw exception
                parameter_factor_++;
            }
        }

        void resetWindow()
        {
            window_ = std::make_shared<ThroughputWindow>(
                time_window_length_ns_ * parameter_factor_, minimum_time_required_ns_ * parameter_factor_);
        }

        int32_t getReadIOTimeout() { return parameter_factor_ * read_io_timeout_ms_; }

    private:
        int32_t slow_io_threshold_{0};
        int32_t read_io_timeout_ms_{0};
        std::chrono::nanoseconds time_window_length_ns_;
        std::chrono::nanoseconds minimum_time_required_ns_;
        int32_t parameter_factor_{1};
        std::shared_ptr<ThroughputWindow> window_;
        std::shared_ptr<SessionConfig> sess_conf_;
    };

}
}

#endif // HDFS_CLIENT_FAILOVERSTRATEGY_FASTSWITCHREAD_H
