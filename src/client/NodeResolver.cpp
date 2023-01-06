
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

#include "NodeResolver.h"
#include "Logger.h"
#include "Metrics.h"
namespace Hdfs {
namespace Internal {

BadNodeStore NodeResolver::badNodeStore;
NodeResolver::NodeResolver(const SessionConfig& conf)
:slowNodeTimeoutMs(conf.getBadNodeStoreSlowTimeout()),failNodeTimeoutMs(conf.getBadNodeStoreFailTimeout()){
}

bool NodeResolver::isExpired(BadNodeInfo info, std::chrono::steady_clock::time_point t) {
    auto expiryTimeInMS = info.state == BadNodeInfo::NodeState::SLOW ? slowNodeTimeoutMs
	                                                             : failNodeTimeoutMs;
    auto duration = t - info.start;
    return duration > std::chrono::milliseconds(expiryTimeInMS);
}

std::optional<DatanodeInfo> NodeResolver::getBestNode(const std::vector<DatanodeInfo>& nodes) {

    std::vector<std::pair<DatanodeInfo,std::chrono::steady_clock::time_point>> slowNodes;
    std::vector<std::pair<DatanodeInfo,std::chrono::steady_clock::time_point>> failedNodes;
    auto current = std::chrono::steady_clock::now();
    int i = 0;
//    std::unique_lock<mutex> lock(badNodeStore.mtx);
    for(auto & node : nodes) {
        BadNodeInfo info;
        // case 1: node not in bad not store, is a good node.
        if(!badNodeStore.nodes.find(node,&info)) {
            if(i > 0) {
               LOG(DEBUG2,"[SLOW NODE] Avoided %i slow nodes.",i);
            }
            return node;
        }

        LOG(DEBUG2,"[SLOW NODE] Node [%s] is in slow node store, will attempt to avoid.",node.getHostName().c_str());
        // if it's expired, remove it from the store, and treat it as a good node.
        if (isExpired(info,current)) {
            LOG(DEBUG2,"[SLOW NODE] Node [%s] has been removed from bad store. Using node.",node.getHostName().c_str());
	        badNodeStore.nodes.erase(node);
            return node;
        }
        // if not, add it to the respective lists for potential processing if necessary
        if(info.state == BadNodeInfo::NodeState::SLOW) {
            slowNodes.push_back({node,info.start});
        } else if (info.state == BadNodeInfo::NodeState::FAILED) {
            failedNodes.push_back({node,info.start});
        }
        ++i;
    }
//    lock.unlock();

    if(!slowNodes.empty()) {
        LOG(DEBUG2,"No known good nodes to use, resorting to slow nodes.");
        // sort by start time.
        std::sort(std::begin(slowNodes),std::end(slowNodes),[](auto & p1, auto& p2){return p1.second < p2.second;});
        return slowNodes.front().first;
    }
    if(!failedNodes.empty()) {
        LOG(DEBUG2,"No known good/slow nodes to use, resorting to known failed nodes.");
        // sort by start time.
        std::sort(std::begin(failedNodes),std::end(failedNodes),[](auto & p1, auto& p2){return p1.second < p2.second;});
        return failedNodes.front().first;
    }
    LOG(DEBUG2,"No known nodes to use at all.");
    return std::nullopt;
}

void NodeResolver::setSlowNode(DatanodeInfo node) {
    setBadNode(node,BadNodeInfo::NodeState::SLOW);
}

void NodeResolver::setFailNode(DatanodeInfo node) {
    setBadNode(node,BadNodeInfo::NodeState::FAILED);
}

void NodeResolver::setBadNode(DatanodeInfo node, BadNodeInfo::NodeState state) {
    BadNodeInfo current;
    auto now = std::chrono::steady_clock::now();
//    std::unique_lock<mutex> lock(badNodeStore.mtx);
    // if the last known status was expired, we overwrite it anyway.
    if(badNodeStore.nodes.find(node,&current) && !isExpired(current,now)) {
        // there are 4 cases
        // current new
        // SLOW    FAIL (promote to fail)
        // SLOW    SLOW (update time)
        // FAIL    SLOW (reject)
        // FAIL    FAIL (update time)

        // we don't want a case where we overwrite a fail status with a slow status.
        if(current.state == BadNodeInfo::NodeState::FAILED && state == BadNodeInfo::NodeState::SLOW) {
            return;
        }
    }
    badNodeStore.nodes.insert(node,{state,now});
}

}
}
