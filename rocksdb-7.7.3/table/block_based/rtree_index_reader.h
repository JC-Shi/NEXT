//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once
#include "table/block_based/index_reader_common.h"
#include "util/hash_containers.h"

namespace ROCKSDB_NAMESPACE {
// Index that allows binary search lookup in a two-level index structure.
class RtreeIndexReader : public BlockBasedTable::IndexReaderCommon {
 public:
  // Read the partition index from the file and create an instance for
  // `RtreeIndexReader`.
  // On success, index_reader will be populated; otherwise it will remain
  // unmodified.
  static Status Create(const BlockBasedTable* table, const ReadOptions& ro,
                       FilePrefetchBuffer* prefetch_buffer, InternalIterator* meta_index_iter, 
                       bool use_cache, bool prefetch, bool pin,
                       BlockCacheLookupContext* lookup_context,
                       std::unique_ptr<IndexReader>* index_reader);

  // return a two-level iterator: first level is on the partition index
  InternalIteratorBase<IndexValue>* NewIterator(
      const ReadOptions& read_options, bool /* disable_prefix_seek */,
      IndexBlockIter* iter, GetContext* get_context,
      BlockCacheLookupContext* lookup_context) override;

  Status CacheDependencies(const ReadOptions& ro, bool pin) override;
  size_t ApproximateMemoryUsage() const override {
    size_t usage = ApproximateIndexBlockMemoryUsage();
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
    usage += malloc_usable_size(const_cast<RtreeIndexReader*>(this));
#else
    usage += sizeof(*this);
#endif  // ROCKSDB_MALLOC_USABLE_SIZE
    // TODO(myabandeh): more accurate estimate of partition_map_ mem usage
    return usage;
  }
 protected:
  uint32_t rtree_height_;

 private:
  RtreeIndexReader(const BlockBasedTable* t,
                       CachableEntry<Block>&& index_block)
      : IndexReaderCommon(t, std::move(index_block)) {}

  // For partition blocks pinned in cache. This is expected to be "all or
  // none" so that !partition_map_.empty() can use an iterator expecting
  // all partitions to be saved here.
  UnorderedMap<uint64_t, CachableEntry<Block>> partition_map_;
//   uint32_t rtree_height_;
};
}  // namespace ROCKSDB_NAMESPACE
