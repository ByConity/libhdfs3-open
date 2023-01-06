
/*
 * Copyright (2022) ByteDance Ltd.
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

#ifndef _HDFS_LIBHDFS3_COMMON_SLIDING_WINDOW_H_
#define _HDFS_LIBHDFS3_COMMON_SLIDING_WINDOW_H_

#include <chrono>
#include <algorithm>
#include <numeric>

namespace Hdfs {
namespace Internal {

template<typename T>
class SlidingWindow
{
    public:
    using Duration = std::chrono::nanoseconds;

    template<typename Rep, typename Period>
    void setDuration(std::chrono::duration<Rep,Period> windowLength_) {
        windowLength = std::chrono::duration_cast<Duration>(windowLength_);
        pruneWindow();
        readyToProcessThroughput = false;
    }

    template<typename Rep, typename Period>
    void addData(std::chrono::duration<Rep,Period> elapsed_, T data_) {
        pruneWindow();
        window.emplace_back(std::chrono::duration_cast<Duration>(elapsed_),data_);
        totalWindowSize = totalWindowSize + data_;
        totalWindowDuration = totalWindowDuration + elapsed_;
    }

    void reset(){
        window.clear();
        readyToProcessThroughput = false;
        totalWindowDuration = {};
        totalWindowSize = {};
    }

    bool isReady() const {
        return readyToProcessThroughput;
    }

    size_t getCount() const {
        return window.size();
    }

    template <typename NewDuration>
    double getThroughput() const {
        return static_cast<double>(totalWindowSize) / std::chrono::duration_cast<NewDuration>(totalWindowDuration).count();
    }

    T getTotal() const {
        T sum{};
        return std::accumulate(std::begin(window)
                    ,std::end(window)
                    ,sum
                    ,[](const T& accum, const auto& rhs)
                    {
                        return accum + rhs.second;
                    }
                    );
    }

    template <typename NewDuration>
    typename NewDuration::rep getTotalDuration() const {
        if(window.empty())
            return {};

        auto start = window[0].first;
        typename NewDuration::rep sum {};
        return std::accumulate(std::begin(window)
                                ,std::end(window)
                                ,sum
                                ,[](const auto &accum, const auto& rhs) {
                                    return accum + std::chrono::duration_cast<NewDuration>(rhs.first).count();
                                });
    }

    private:

    void pruneWindow() {

        auto it = std::begin(window);
        while(totalWindowDuration > windowLength && it != std::end(window)) {
            totalWindowDuration -= it->first;
            totalWindowSize -= it->second;
            ++it;
            readyToProcessThroughput = true;
        }
        window.erase(std::begin(window),it);
    }

    std::vector<std::pair<Duration,T>> window;
    Duration windowLength = 5000ms;

    // used only to tabulate.
    Duration totalWindowDuration;
    T totalWindowSize {};
    bool readyToProcessThroughput = false;
};

}
}
#endif
