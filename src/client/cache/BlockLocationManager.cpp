
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


#include "client/cache/BlockLocationManager.h"

#include <algorithm>
#include <iostream>
#include <string>
#include <vector>
#include "Logger.h"
// Do not support getblocklocation for block being writing.
// If cache_miss, the block_location_manager does not responsible fror update blockinfo.

namespace Hdfs
{
namespace Internal
{
    BlockLocationManager::BlockLocationManager(uint32_t max_cache_size) : cache_size(max_cache_size), lru_cache(max_cache_size)
    {
        if(cache_size ==0 ) {
            LOG(FATAL, "block location cache size cannot be zero");
        }
        clean_cache_thread = std::thread([this]() {
            while (!this->should_stop.load())
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(kCleanPeriodMs));
                this->cleanExpiredCache(kCacheExpireBatch, kCacheExpireMs);
            }
        });
    }

    BlockLocationManager::~BlockLocationManager()
    {
        should_stop.store(true);
        clean_cache_thread.join();
    }

    std::optional<std::shared_ptr<LocatedBlocks>> BlockLocationManager::getBlockLocation(const std::string & path, uint64_t offset)
    {
        std::optional<std::shared_ptr<BlockCacheItem>> cache_entry;
        std::lock_guard<std::mutex> lock(cache_mutex);
        cache_entry = lru_cache.get(path);
        if (!cache_entry.has_value())
        {
            return std::nullopt;
        }
        else
        {
            std::shared_ptr<LocatedBlocks> lbs = (*cache_entry)->blocks_;
            (*cache_entry)->last_update_time_ = std::chrono::steady_clock::now();
            if (lbs->findBlock(offset))
            {
                return std::optional<std::shared_ptr<LocatedBlocks>>{std::move(lbs)};
            }
        }
        return std::nullopt;
    }


    std::optional<std::shared_ptr<LocatedBlocks>> BlockLocationManager::getBlockLocationCopy(const std::string & path, uint64_t offset)
    {
        std::optional<std::shared_ptr<BlockCacheItem>> cache_entry;
        std::lock_guard<std::mutex> lock(cache_mutex);
        cache_entry = lru_cache.get(path);
        if (!cache_entry.has_value())
        {
            return std::nullopt;
        }
        else
        {
            std::shared_ptr<LocatedBlocks> lbs = (*cache_entry)->blocks_;
            (*cache_entry)->last_update_time_ = std::chrono::steady_clock::now();
            if (lbs->findBlock(offset))
            {
                return std::optional<std::shared_ptr<LocatedBlocks>>{(*cache_entry)->blocks_->deep_copy()};
            }
        }
        return std::nullopt;
    }

    size_t BlockLocationManager::setBlockLocationCache(const std::string & path, std::shared_ptr<LocatedBlocks> new_located_blocks)
    {
        assert(new_located_blocks.get() != nullptr);
        std::optional<std::shared_ptr<BlockCacheItem>> cached_entry;
        std::shared_ptr<BlockCacheItem> new_entry;
        std::lock_guard<std::mutex> lock(cache_mutex);

        cached_entry = lru_cache.get(path);

        if (cached_entry.has_value())
        {
            mergeLocatedBlocks(new_located_blocks, (*cached_entry)->blocks_);
            (*cached_entry)->blocks_ = new_located_blocks;
            (*cached_entry)->last_update_time_ = std::chrono::steady_clock::now();
        }
        else
        {
            new_entry = std::make_shared<BlockCacheItem>(new_located_blocks);
            new_entry->last_update_time_ = std::chrono::steady_clock::now();
            lru_cache.insert(path, new_entry);
        }
        return lru_cache.size();

    }


    void BlockLocationManager::mergeLocatedBlocks(
        std::shared_ptr<LocatedBlocks> dst_located_blocks, std::shared_ptr<LocatedBlocks> src_located_blocks)
    {

//        LOG(DEBUG2, "[LOCATION CACHE] merge cache" );
        assert(dst_located_blocks.get() != nullptr);
        assert(src_located_blocks.get() != nullptr);
        assert(src_located_blocks.get() != dst_located_blocks.get());
        std::vector<LocatedBlock> & dst_block_list = dst_located_blocks->getBlocks();
        std::vector<LocatedBlock> & src_block_list = src_located_blocks->getBlocks();

        dst_block_list.insert(dst_block_list.end(), src_block_list.begin(), src_block_list.end());
        // The block_info in dst_block_list is newer, use stable_sort() to keep the relative order.
        stable_sort(dst_block_list.begin(), dst_block_list.end());
        // Remove the duplicated located_blocks
        auto index_it = std::unique(dst_block_list.begin(), dst_block_list.end());
        dst_block_list.erase(index_it, dst_block_list.end());

        if (dst_located_blocks->getFileLength() < src_located_blocks->getFileLength())
        {
            dst_located_blocks->setFileLength(src_located_blocks->getFileLength());
            dst_located_blocks->setLastBlock(src_located_blocks->getLastBlock());
            dst_located_blocks->setIsLastBlockComplete(src_located_blocks->isLastBlockComplete());
            dst_located_blocks->setUnderConstruction(src_located_blocks->isUnderConstruction());
        }
    }

    void BlockLocationManager::removeBlockLocationCache(const std::string & path)
    {
//        LOG(DEBUG2, "[LOCATION CACHE] remove location cache for %s", path.c_str());
        std::lock_guard<std::mutex> lock(cache_mutex);
        lru_cache.remove(path);
    }


    void BlockLocationManager::cleanExpiredCache(size_t num, uint64_t expireInMs)
    {
        auto ddl = std::chrono::steady_clock::now() - std::chrono::milliseconds(expireInMs);
        std::lock_guard<std::mutex> lock(cache_mutex);
        while (num > 0 && !should_stop.load())
        {
            --num;
            auto opt = lru_cache.last(); // a littlt bit ugly. check LruCache.h.
            if (!opt.has_value())
            {
                return;
            }
            std::shared_ptr<BlockCacheItem> it = opt.value();
            if (it->last_update_time_ < ddl)
            {
                lru_cache.evict();
            }
            else
            {
                return;
            }
        }
    }


}
}