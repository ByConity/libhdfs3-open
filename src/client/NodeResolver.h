
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

#ifndef _HDFS_LIBHDFS3_CLIENT_NODERESOLVER_H_
#define _HDFS_LIBHDFS3_CLIENT_NODERESOLVER_H_

#include <chrono>
#include <vector>
#include <optional>
#include "server/DatanodeInfo.h"
#include "common/LruMap.h"
#include "common/SessionConfig.h"

namespace Hdfs {
namespace Internal {

struct BadNodeInfo {
    enum class NodeState {
        FAILED,
        SLOW
    };
    NodeState state;
    std::chrono::steady_clock::time_point start;
};

struct BadNodeStore {
    // no point using a shared mutex with read write locks because getBestNode needs to also remove slowNodes and failedNodes based on time.
    std::mutex mtx;
    LruMap<DatanodeInfo,BadNodeInfo> nodes;
};

class NodeResolver {
public:
    NodeResolver(const SessionConfig& conf);
    std::optional<DatanodeInfo> getBestNode(const std::vector<DatanodeInfo>& nodes);
    void setSlowNode(DatanodeInfo node);
    void setFailNode(DatanodeInfo node);
private:
    bool isExpired(BadNodeInfo info, std::chrono::steady_clock::time_point t);
    void setBadNode(DatanodeInfo node, BadNodeInfo::NodeState state);
    uint32_t slowNodeTimeoutMs;
    uint32_t failNodeTimeoutMs;
    static BadNodeStore badNodeStore;
};

}
}

#endif
