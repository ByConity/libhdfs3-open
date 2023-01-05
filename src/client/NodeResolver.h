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
