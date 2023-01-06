
/*
 * This file may have been modified by ByteDance Ltd. (“ Bytedance's Modifications”).
 * All Bytedance's Modifications are Copyright (2023) ByteDance Ltd..
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
#ifndef _HDFS_LIBHDFS3_CLIENT_INPUTSTREAMIMPL_H_
#define _HDFS_LIBHDFS3_CLIENT_INPUTSTREAMIMPL_H_

#include <boost/core/noncopyable.hpp>
#include "BlockReader.h"
#include "ExceptionInternal.h"
#include "FileSystem.h"
#include "Hash.h"
#include "HdfsEvent.h"
#include "InputStreamInter.h"
#include "Memory.h"
#include "NodeResolver.h"
#include "PeerCache.h"
#include "SessionConfig.h"
#include "SlidingWindow.h"
#include "Unordered.h"
#include "cache/BlockLocationManager.h"
#include "failover_strategy/FastSwitchReadStrategy.h"
#include "platform.h"
#include "rpc/RpcAuth.h"
#include "server/Datanode.h"
#include "server/LocatedBlock.h"
#include "server/LocatedBlocks.h"
#ifdef MOCK
#include "TestDatanodeStub.h"
#endif

namespace Hdfs {
namespace Internal
{
    /**
 * A input stream used read data from hdfs.
 */
    class InputStreamImpl : public InputStreamInter, public std::enable_shared_from_this<InputStreamImpl>
    {
    public:
        InputStreamImpl();
        ~InputStreamImpl();

        /**
     * Open a file to read
     * @param fs hdfs file system.
     * @param path the file to be read.
     * @param verifyChecksum verify the checksum.
     */
        void open(shared_ptr<FileSystemInter> fs, const char * path, bool verifyChecksum, hdfsEventCallBack cb);

        /**
     * To read data from hdfs.
     * @param buf the buffer used to filled.
     * @param size buffer size.
     * @return return the number of bytes filled in the buffer, it may less than size.
     */
        int32_t read(char * buf, int32_t size);

    /**
     * To read data from hdfs.
     * @param buf the buffer used to filled.
     * @param size buffer size.
     * @return return the number of bytes filled in the buffer, it may less than size.
     */
    int32_t pread(char * buf, int32_t size);

    /**
     * To read data from hdfs, block until get the given size of bytes.
     * @param buf the buffer used to filled.
     * @param size the number of bytes to be read.
     */
    void readFully(char * buf, int64_t size);

    int64_t available();

    /**
     * To move the file point to the given position.
     * @param pos the given position.
     */
    void seek(int64_t pos);

    /**
     * To get the current file point position.
     * @return the position of current file point.
     */
    int64_t tell();

    /**
     * Close the stream.
     */
    void close();

    /**
     * Convert to a printable string
     *
     * @return return a printable string
     */
    std::string toString();

protected:
    void setSlowNode(const DatanodeInfo & node);
    void setFailedNode(const DatanodeInfo & node);
    bool choseBestNode();
    bool isLocalNode();
    bool isLocalNode(const DatanodeInfo& node);
    int32_t readInternal(char * buf, int32_t size, bool pread);
    int32_t readOneBlock(char * buf, int32_t size, bool shouldUpdateMetadataOnFailure, bool pread);

    // hedge read. readOneBlockAsyncPRead is using coroutine. readOneBlockAsyncPReadV2 is using callback style.
    int32_t readOneBlockAsyncPRead(char * buf, int32_t size, bool shouldUpdateMetaDataOnFailure );
    int32_t readOneBlockAsyncPReadV2(char * buf, int32_t size, bool shouldUpdateMetaDataOnFailure );

    int64_t getFileLength();
    int64_t readBlockLength(const LocatedBlock & b);
    void checkStatus();
    void openInternal(shared_ptr<FileSystemInter> fs, const char * path, bool verifyChecksum);
    void readFullyInternal(char * buf, int64_t size);
    void seekInternal(int64_t pos);
    void seekToBlock(const LocatedBlock & lb);
    void setupBlockReader(bool temporaryDisableLocalRead, int32_t size, bool pread);
    void updateBlockInfos();
    void setCurrentNode(DatanodeInfo node);
    void closeShared();
protected:
    bool closed;
    bool localRead;
    bool readFromUnderConstructedBlock;
    bool verify;
    DatanodeInfo curNode;
    exception_ptr lastError;
    FileStatus fileInfo;
    int maxGetBlockInfoRetry;
    int64_t cursor;
    int64_t endOfCurBlock;
    int64_t lastBlockBeingWrittenLength;
    int64_t totalBytesRead;
    int64_t prefetchSize;
    PeerCache *peerCache;
    NodeResolver *nodeResolver;
    RpcAuth auth;
    shared_ptr<BlockReader> blockReader;
    shared_ptr<FileSystemInter> filesystem;
    shared_ptr<LocatedBlock> curBlock;
    shared_ptr<LocatedBlocks> lbs;
    shared_ptr<SessionConfig> conf;
    std::string path;
    std::set<DatanodeInfo> failedNodes;
    std::set<DatanodeInfo> slowNodes;
    std::vector<char> localReaderBuffer;
    //    SlidingWindow<int32_t> throughputWindow;
    BlockLocationManager & block_location_manager_;
    std::shared_ptr<FastSwitchReadStrategy> switch_read_strategy_;
    bool enableLocationCache;
    hdfsEventCallBack hdfs_event_callback;

    // hedgeRead
//    std::set<DatanodeInfo> ignored;
    bool enableHedgedRead;
    std::mutex nodeMutex;
#ifdef MOCK
private:
    Hdfs::Mock::TestDatanodeStub * stub;
#endif
};

//class CloudFileSystemInputStreamImpl : public InputStreamImpl
//{
//};
//
//class HedgedExectuor
//{
//public:
//    using TokenT = size_t;
//    template <typename F>
//    void schedule(const F & f);
//
//
//    void cancel(const TokenT token);
//};




}
}

#endif /* _HDFS_LIBHDFS3_CLIENT_INPUTSTREAMIMPL_H_ */
