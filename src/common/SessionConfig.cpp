/*
 * This file may have been modified by Bytedance Ltd. and/or its affiliates (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) Bytedance Ltd. and/or its affiliates.
 */

/********************************************************************
 * 2014 -
 * open source under Apache License Version 2.0
 ********************************************************************/
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "Exception.h"
#include "ExceptionInternal.h"
#include "Function.h"
#include "SessionConfig.h"
#include "rpc/RpcAuth.h"
#include <sstream>

#define ARRAYSIZE(A) (sizeof(A) / sizeof(A[0]))

namespace Hdfs {
namespace Internal {

template<typename T>
static void CheckRangeGE(const char * key, T const & value, T const & target) {
    if (!(value >= target)) {
        std::stringstream ss;
        ss.imbue(std::locale::classic());
        ss << "Invalid configure item: \"" << key << "\", value: " << value
           << ", expected value should be larger than " << target;
        THROW(HdfsConfigInvalid, "%s", ss.str().c_str());
    }
}

template<typename T>
static void CheckMultipleOf(const char * key, const T & value, int unit) {
    if (value <= 0 || value % unit != 0) {
        THROW(HdfsConfigInvalid, "%s should be larger than 0 and be the multiple of %d.", key, unit);
    }
}

int32_t parseProtection(std::string &str) {
    if (0 == strcasecmp(str.c_str(), "authentication")) {
        return Protection::AUTH;
    } else if (0 == strcasecmp(str.c_str(), "privacy")) {
        return Protection::CONF;
    } else if (0 == strcasecmp(str.c_str(), "integrity")) {
        return Protection::INT;
    } else {
        THROW(InvalidParameter, "SessionConfig: Unknown protection mechanism type: %s",
              str.c_str());
    }

}

SessionConfig::SessionConfig(const Config & conf) {
    ConfigDefault<bool> boolValues [] = {
        {
            &rpcTcpNoDelay, "rpc.client.connect.tcpnodelay", true
        }, {
            &readFromLocal, "dfs.client.read.shortcircuit", true
        }, {
            &addDatanode, "output.replace-datanode-on-failure", true
        }, {
            &addDatanodeBest, "output.replace-datanode-on-failure.best-effort", true
        },{
            &notRetryAnotherNode, "input.notretry-another-node", false
        }, {
            &useMappedFile, "input.localread.mappedfile", false
        }, {
            &legacyLocalBlockReader, "dfs.client.use.legacy.blockreader.local", false
        }, {
            &badNodeStore, "input.badnodestore.use", true
        }, {
            &fastSwitchRead, "input.fastswitchread.use", true
        }, {
            &encryptedDatanode, "dfs.encrypt.data.transfer", false
        }, {
            &secureDatanode, "dfs.block.access.token.enable", false       
       	}, {
            &useDatanodeHostname, "dfs.client.use.datanode.hostname", false
        }, {
            &useInternalCredentialFormat, "dfs.use.internal.credential.format", true
        }, {
            &enableFastSwitchRead, "dfs.client.switch.read.enable", true
        }, {
            &enableInternalMetrics, "dfs.client.metrics.enable", false
        },{
            &internalMetricsUseDomainSocket, "dfs.client.metrics.use_domain_socket", true
        }, {
            &enableFastSwitchRandomDelay, "dfs.read.random_delay.enable", false
        }, {
            &enableBlockLocationCache, "dfs.block-location-cache.enable", true
        }, {
            &verifyChecksum, "dfs.read.checksum.enable", true
        }, {
            &enableSocketCache, "dfs.socketcache.enable", true
        }, {
            &enableHedgedRead, "dfs.read.hedge_read.enable", false
        }
    };
    ConfigDefault<int32_t> i32Values[] = {
        {
            &rpcMaxIdleTime, "rpc.client.max.idle", 10 * 1000, bind(CheckRangeGE<int32_t>, _1, _2, 1)
        }, {
            &rpcPingTimeout, "rpc.client.ping.interval", 10 * 1000
        }, {
            &rpcConnectTimeout, "rpc.client.connect.timeout", 600 * 1000
        }, {
            &rpcReadTimeout, "rpc.client.read.timeout", 3600 * 1000
        }, {
            &rpcWriteTimeout, "rpc.client.write.timeout", 3600 * 1000
        }, {
            &rpcSocketLingerTimeout, "rpc.client.socekt.linger.timeout", -1
        }, {
            &rpcMaxRetryOnConnect, "rpc.client.connect.retry", 10, bind(CheckRangeGE<int32_t>, _1, _2, 1)
        }, {
            &rpcTimeout, "rpc.client.timeout", 3600 * 1000
        }, {
            &defaultReplica, "dfs.default.replica", 3, bind(CheckRangeGE<int32_t>, _1, _2, 1)
        }, {
            &inputConnTimeout, "input.connect.timeout", 600 * 1000
        }, {
            &inputReadTimeout, "input.read.timeout", 3600 * 1000
        }, {
            &inputReadOneBlockTimeout, "input.read.readoneblock.timeout", -1
        }, {
            &inputReadThroughputWindowDuration, "input.read.throughput.window.duration", 5000 //milliseconds
        }, {
            &inputWriteTimeout, "input.write.timeout", 3600 * 1000
        }, {
            &badNodeStoreSlowTimeout, "input.badnodestore.slow.timeout", 3600 * 1000
        }, {
            &badNodeStoreFailTimeout, "input.badnodestore.fail.timeout", 3600 * 1000
        }, {
            &localReadBufferSize, "input.localread.default.buffersize", 1 * 1024 * 1024, bind(CheckRangeGE<int32_t>, _1, _2, 1)
        }, {
            &prefetchSize, "dfs.prefetchsize", 10, bind(CheckRangeGE<int32_t>, _1, _2, 1)
        }, {
            &maxGetBlockInfoRetry, "input.read.getblockinfo.retry", 3, bind(CheckRangeGE<int32_t>, _1, _2, 1)
        }, {
            &maxLocalBlockInfoCacheSize, "input.localread.blockinfo.cachesize", 1000, bind(CheckRangeGE<int32_t>, _1, _2, 1)
        }, {
            &maxReadBlockRetry, "input.read.max.retry", 60, bind(CheckRangeGE<int32_t>, _1, _2, 1)
        }, {
            &chunkSize, "output.default.chunksize", 512, bind(CheckMultipleOf<int32_t>, _1, _2, 512)
        }, {
            &packetSize, "output.default.packetsize", 64 * 1024
        }, {
            &blockWriteRetry, "output.default.write.retry", 10, bind(CheckRangeGE<int32_t>, _1, _2, 1)
        }, {
            &outputConnTimeout, "output.connect.timeout", 600 * 1000
        }, {
            &outputReadTimeout, "output.read.timeout", 3600 * 1000
        }, {
            &outputWriteTimeout, "output.write.timeout", 3600 * 1000
        }, {
            &closeFileTimeout, "output.close.timeout", 3600 * 1000
        }, {
            &packetPoolSize, "output.packetpool.size", 1024
        }, {
            &heartBeatInterval, "output.heeartbeat.interval", 10 * 1000
        }, {
            &rpcMaxHARetry, "dfs.client.failover.max.attempts", 15, bind(CheckRangeGE<int32_t>, _1, _2, 0)
        }, {
            &maxFileDescriptorCacheSize, "dfs.client.read.shortcircuit.streams.cache.size", 256, bind(CheckRangeGE<int32_t>, _1, _2, 0)
        }, {
            &socketCacheExpiry, "dfs.client.socketcache.expiryMsec", 3000, bind(CheckRangeGE<int32_t>, _1, _2, 0)
        }, {
            &socketCacheCapacity, "dfs.client.socketcache.capacity", 4096, bind(CheckRangeGE<int32_t>, _1, _2, 0)
        }, {
            &cryptoBufferSize, "hadoop.security.crypto.buffer.size", 8192
        }, {
            &httpRequestRetryTimes, "kms.send.request.retry.times", 4
        }, {
            &fsrIoTimeoutMs, "dfs.socket.read.ms.timeout", 6000
        }, {
            &fsrSlowIoThresholdBps, "dfs.slowRead.bytesPerSec.threshold", 3145728
        }, {
            &hedgedReadIntervalMs, "dfs.read.hedge_read.interval_ms", 200
        }
    };
    ConfigDefault<int64_t> i64Values [] = {
        {
            &defaultBlockSize, "dfs.default.blocksize", 256 * 1024 * 1024, bind(CheckMultipleOf<int64_t>, _1, _2, 512)
        }, {
            &curlTimeout, "kms.send.request.timeout", 20L
        }, {
            &inputReadThroughputSlowThreshold, "input.read.throughput.slowthreshold", 24 * 1024 * 1024//MBps.
        }, {
            &fsrTimewindowLengthNs, "dfs.read.timeWindow.ns.length", 5LL * 1000 * 1000000 // set a smaller window size
        }, {
            &fsrMinimunTimeNs, "dfs.read.minimum.ns.sample-time", 1500LL * 1000000
        }, {
            &fastSwtichRandomDelayLowerBoundMs, "dfs.read.random_delay.lower_bound_ms", 1000
        }, {
            &fastSwtichRandomDelayUpperBoundMs, "dfs.read.random_delay.upper_bound_ms",2000
        }, {
            &blockLocationCacheSize, "dfs.block-location-cache.size", 100000
        }, {
            &hedgedReadMaxWaitTime, "dfs.read.hedge_read.max_wait_ms", 30000
        }
    };

    std::string authMethod;


    ConfigDefault<std::string> strValues [] = {
        {&defaultUri, "dfs.default.uri", "hdfs://localhost:8020" },
        {&rpcAuthMethod, "hadoop.security.authentication", "simple" },
        {&kerberosCachePath, "hadoop.security.kerberos.ticket.cache.path", "" },
        {&logSeverity, "dfs.client.log.severity", "INFO" },
        {&domainSocketPath, "dfs.domain.socket.path", ""},
        {&kmsUrl, "dfs.encryption.key.provider.uri", "" },
        {&rpcProtectionStr, "hadoop.rpc.protection", ""},
        {&dataProtectionStr, "dfs.data.transfer.protection", ""},
        {&kmsAuthMethod, "hadoop.kms.authentication.type", "simple" },
        {&internalMetricsPrefix, "dfs.client.metrics.prefix", "cnch.hdfs.client"},
        {&internalMetricsTags, "dfs.client.metrics.tags", ""},
        {&internalMetricsSocketPath, "dfs.client.metrics.socket_path", "/opt/tmp/sock/metric2.sock"}
    };

    for (size_t i = 0; i < ARRAYSIZE(boolValues); ++i) {
        *boolValues[i].variable = conf.getBool(boolValues[i].key,
                                               boolValues[i].value);

        if (boolValues[i].check) {
            boolValues[i].check(boolValues[i].key, *boolValues[i].variable);
        }
    }

    for (size_t i = 0; i < ARRAYSIZE(i32Values); ++i) {
        *i32Values[i].variable = conf.getInt32(i32Values[i].key,
                                               i32Values[i].value);

        if (i32Values[i].check) {
            i32Values[i].check(i32Values[i].key, *i32Values[i].variable);
        }
    }

    for (size_t i = 0; i < ARRAYSIZE(i64Values); ++i) {
        *i64Values[i].variable = conf.getInt64(i64Values[i].key,
                                               i64Values[i].value);

        if (i64Values[i].check) {
            i64Values[i].check(i64Values[i].key, *i64Values[i].variable);
        }
    }

    for (size_t i = 0; i < ARRAYSIZE(strValues); ++i) {
        *strValues[i].variable = conf.getString(strValues[i].key,
                                                strValues[i].value);

        if (strValues[i].check) {
            strValues[i].check(strValues[i].key, *strValues[i].variable);
        }
    }

    if (rpcProtectionStr.length() > 0) {
        rpcProtection = parseProtection(rpcProtectionStr);
    } else {
        rpcProtection = 0;
    }
    if (dataProtectionStr.length() > 0) {
        dataProtection = parseProtection(dataProtectionStr);
    } else {
        dataProtection = 0;
    }
}

}
}
