
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


// Adapted from native hdfs client.
// 1. removed over-complexed LRU implementation.
// 2. removed folly.
//
// Created by renjie on 2021-04-23.
//

#ifndef HDFS_CLIENT_BLOCKLOCATIONMANAGER_H
#define HDFS_CLIENT_BLOCKLOCATIONMANAGER_H

#include <algorithm>
#include <atomic>
#include <chrono>
#include <mutex>
#include <optional>
#include <thread>
#include "client/cache/LruCache.h"
#include "server/LocatedBlocks.h"
namespace Hdfs
{
namespace Internal
{
    // Do not support getblocklocation for block being writing.
    // If cache_miss, the block_location_manager does not responsible for update blockinfo.
    const uint64_t kCleanPeriodMs = 50;
    const uint64_t kCacheExpireMs = 5 * 60 * 1000;
    const uint64_t kCacheExpireBatch = 1000;
    struct BlockCacheItem
    {
        BlockCacheItem(std::shared_ptr<LocatedBlocks> located_blocks) : blocks_(located_blocks) { }

        // If not updated for long time, the cache will be evicted.
        std::chrono::steady_clock::time_point last_update_time_;
        std::shared_ptr<LocatedBlocks> blocks_;
    };

    class BlockLocationManager
    {
    public:
        static BlockLocationManager & GetSingleton(uint32_t cacheSize )
        {
            static BlockLocationManager block_location_manager(cacheSize);
            return block_location_manager;
        }

        /*
        * Init blocklocation manager.
        *
        * @param max_cache_size maximum size of the cache map.  Once the map size exceeds
        *        maxSize, the map will begin to evict.
        *
        * @param cache_expire_time_sec The expired time of cache entry.
        *        Once expired, the cache entry will be eliminated
        *
        */
        BlockLocationManager(uint32_t max_cache_size);

        ~BlockLocationManager();

        /*
         * Get LocatedBlocks according to path and offset
         *
         * @param path file path to get block_location.
         *
         * @param offset The offset of block in path.
         *
         * @return null if cache miss or the locatedBlocks which contains the requested block info.
         *
         */
        std::optional<std::shared_ptr<LocatedBlocks>> getBlockLocation(const std::string & path, uint64_t offset);
        /*
         * Deep copy the entry in cache.
         */
        std::optional<std::shared_ptr<LocatedBlocks>> getBlockLocationCopy(const std::string & path, uint64_t offset);


        /*
         * Set the LocatedBlocks block_location cache for path.
         * If the path has already set in the cache,
         * This function will try to merge the block_list of the two LocatedBlocks
         *
         * @param path file_path to set block_location.
         *
         * @param new_located_blocks The block_location cache to set
         *
         */
        size_t setBlockLocationCache(const std::string & path, std::shared_ptr<LocatedBlocks> new_located_blocks);



        /*
         * remove entry.
         */
        void removeBlockLocationCache(const std::string & path);


        /*
         * Merge the src_located_blocks into the dst_located_blocks.
         * This will sort the block list by offset and remove these duplicated blocks
         * If there is duliplicated block, blockinfo in dst will overwrite blockinfo in src.
         *
         * @param dst_located_blocks
         *
         * @param src_located_blocks
         *
         */
        static void
        mergeLocatedBlocks(std::shared_ptr<LocatedBlocks> dst_located_blocks, std::shared_ptr<LocatedBlocks> src_located_blocks);

    private:
        void cleanExpiredCache(size_t num, uint64_t expireInMs);

        std::mutex cache_mutex;
        size_t cache_size;
        LruCache<std::string, std::shared_ptr<BlockCacheItem>> lru_cache;
        std::atomic_bool should_stop{false};
        std::thread clean_cache_thread;
    };

}
}


#endif //HDFS_CLIENT_BLOCKLOCATIONMANAGER_H
