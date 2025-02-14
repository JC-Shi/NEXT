//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "table/block_based/onedrtree_sec_index_reader.h"

#include "file/random_access_file_reader.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/block_based/onedrtree_sec_index_iterator.h"

#include "table/block_fetcher.h"
#include "table/meta_blocks.h"
#include "util/z_curve.h"

namespace ROCKSDB_NAMESPACE {
Status OneDRtreeSecIndexReader::Create(
    const BlockBasedTable* table, const ReadOptions& ro,
    FilePrefetchBuffer* prefetch_buffer, InternalIterator* meta_index_iter, bool use_cache, bool prefetch,
    bool pin, BlockCacheLookupContext* lookup_context,
    std::unique_ptr<IndexReader>* index_reader) {
  assert(table != nullptr);

  const BlockBasedTable::Rep* rep = table->get_rep();
  assert(rep != nullptr);
  assert(!pin || prefetch);
  assert(index_reader != nullptr);

  CachableEntry<Block> index_block;
  // std::cout << "create rtreesecindexreader" << std::endl;
  if (prefetch || !use_cache) {
    // std::cout << "start reading index block" << std::endl;

    const Status s = 
          ReadSecIndexBlock(table, prefetch_buffer, ro, use_cache,
                          /*get_context=*/nullptr, lookup_context, &index_block,
                          meta_index_iter);
    

    if (!s.ok()) {
      return s;
    }

    if (use_cache && !pin) {
      index_block.Reset();
    }
  }

  index_reader->reset(new OneDRtreeSecIndexReader(table, std::move(index_block)));

  // std::cout << "get rtree index meta block" << std::endl;

  // Get metadata block
  BlockHandle meta_handle;

  Status s =
        FindMetaBlock(meta_index_iter, kRtreeSecondaryIndexMetadataBlock, &meta_handle);
  

  // std::cout << s.ToString() << std::endl;
  if (!s.ok()) {
    // TODO: log error
    return Status::OK();
  }

  RandomAccessFileReader* const file = rep->file.get();
  const Footer& footer = rep->footer;
  const ImmutableOptions& ioptions = rep->ioptions;
  const PersistentCacheOptions& cache_options = rep->persistent_cache_options;
  MemoryAllocator* const memory_allocator =
      GetMemoryAllocator(rep->table_options);



  // Read contents for the blocks
  BlockContents meta_contents;
  BlockFetcher meta_block_fetcher(
      file, prefetch_buffer, footer, ReadOptions(), meta_handle,
      &meta_contents, ioptions, true /*decompress*/,
      true /*maybe_compressed*/, BlockType::kRtreeIndexMetadata,
      UncompressionDict::GetEmptyDict(), cache_options, memory_allocator);
  s = meta_block_fetcher.ReadBlockContents();
  if (!s.ok()) {
    return s;
  }

  // std::cout << "finish reading rtree index meta block" << std::endl;

  Slice& meta_data = meta_contents.data;

  uint32_t rtree_height = 0;

  if (!GetVarint32(&meta_data, &rtree_height)) {
    s = Status::Corruption(
        "Corrupted prefix meta block: unable to read from it.");
  }

  OneDRtreeSecIndexReader* const onedrtree_index_reader =
        static_cast<OneDRtreeSecIndexReader*>(index_reader->get());
    onedrtree_index_reader->rtree_height_ = rtree_height;
    onedrtree_index_reader->meta_index_iterator_ = meta_index_iter;
  
  // std::cout << "Rtree_height: " << rtree_index_reader->rtree_height_ << std::endl;


  // std::cout << "finished creating RtreeIndexReader" << std::endl;

  return Status::OK();
}

InternalIteratorBase<IndexValue>* OneDRtreeSecIndexReader::NewIterator(
    const ReadOptions& read_options, bool /* disable_prefix_seek */,
    IndexBlockIter* iter, GetContext* get_context,
    BlockCacheLookupContext* lookup_context) {
  // std::cout << "rtreesecindexreader::newiterator" << std::endl;
  const bool no_io = (read_options.read_tier == kBlockCacheTier);
  CachableEntry<Block> index_block;
  const Status s = 
        GetOrReadSecIndexBlock(no_io, read_options.rate_limiter_priority,
                            get_context, lookup_context, &index_block,
                            meta_index_iterator_);
  if (!s.ok()) {
    if (iter != nullptr) {
      iter->Invalidate(s);
      return iter;
    }

    return NewErrorInternalIterator<IndexValue>(s);
  }

  const BlockBasedTable::Rep* rep = table()->rep_;
  InternalIteratorBase<IndexValue>* it = nullptr;

  Statistics* kNullStats = nullptr;
  // Filters are already checked before seeking the index
  if (!partition_map_.empty()) {
    // std::cout << "partition_map_ not empty" << std::endl;
    // We don't return pinned data from index blocks, so no need
    // to set `block_contents_pinned`.
    it = NewTwoLevelIterator(
        new BlockBasedTable::PartitionedIndexIteratorState(table(),
                                                           &partition_map_),
        index_block.GetValue()->NewIndexIterator(
            internal_comparator()->user_comparator(),
            rep->get_global_seqno(BlockType::kIndex), nullptr, kNullStats, true,
            index_has_first_key(), index_key_includes_seq(),
            index_value_is_full()));
  } else {
    // std::cout << "partition_map_ empty" << std::endl;
    ReadOptions ro;
    ro.fill_cache = read_options.fill_cache;
    ro.deadline = read_options.deadline;
    ro.io_timeout = read_options.io_timeout;
    ro.adaptive_readahead = read_options.adaptive_readahead;
    ro.async_io = read_options.async_io;
    ro.rate_limiter_priority = read_options.rate_limiter_priority;
    // std::cout << "adding iterator context" << std::endl;
    ro.iterator_context = read_options.iterator_context;
    ro.is_secondary_index_scan = read_options.is_secondary_index_scan;
    ro.is_secondary_index_spatial = read_options.is_secondary_index_spatial;
    ro.found_sec_blkhandle = read_options.found_sec_blkhandle;
    // std::cout << "rtree_sec_index_reader::" << (ro.iterator_context == nullptr) << std::endl;
    // RtreeIteratorContext* context =
    //     reinterpret_cast<RtreeIteratorContext*>(ro.iterator_context);
    // std::cout << context->query_mbr << std::endl;
    // std::cout << "finish adding iterator context" << std::endl;

    // We don't return pinned data from index blocks, so no need
    // to set `block_contents_pinned`.
    // ZComparator4SecondaryIndex cmp;
    // std::unique_ptr<InternalIteratorBase<IndexValue>> index_iter(
    //     index_block.GetValue()->NewIndexIterator(
    //         &cmp,
    //         rep->get_global_seqno(BlockType::kIndex), nullptr, kNullStats, true,
    //         index_has_first_key(), index_key_includes_seq(),
    //         index_value_is_full()));
    // std::cout << "three bools: " << index_has_first_key() << "; " << index_key_includes_seq() << "; " << index_value_is_full() << std::endl;
    std::unique_ptr<InternalIteratorBase<IndexValue>> index_iter(
        index_block.GetValue()->NewIndexIterator(
            internal_comparator()->user_comparator(),
            rep->get_global_seqno(BlockType::kIndex), nullptr, kNullStats, true,
            index_has_first_key(), index_key_includes_seq(),
            index_value_is_full()));

    // std::cout << "rtree_index_reader rtree_height_: " << rtree_height_ << std::endl;

    it = new OneDRtreeSecIndexIterator(
        table(), ro, *internal_comparator(), std::move(index_iter),
        lookup_context ? lookup_context->caller
                       : TableReaderCaller::kUncategorized, rtree_height_);
  }

  assert(it != nullptr);
  index_block.TransferTo(it);

  return it;

  // TODO(myabandeh): Update TwoLevelIterator to be able to make use of
  // on-stack BlockIter while the state is on heap. Currentlly it assumes
  // the first level iter is always on heap and will attempt to delete it
  // in its destructor.
}
Status OneDRtreeSecIndexReader::CacheDependencies(const ReadOptions& ro,
                                               bool pin) {
  // Before read partitions, prefetch them to avoid lots of IOs
  BlockCacheLookupContext lookup_context{TableReaderCaller::kPrefetch};
  const BlockBasedTable::Rep* rep = table()->rep_;
  IndexBlockIter biter;
  BlockHandle handle;
  Statistics* kNullStats = nullptr;

  CachableEntry<Block> index_block;
  {
    Status s = GetOrReadSecIndexBlock(false /* no_io */, ro.rate_limiter_priority,
                                    nullptr /* get_context */, &lookup_context,
                                    &index_block, meta_index_iterator_);      
    

    if (!s.ok()) {
      return s;
    }
  }

  // We don't return pinned data from index blocks, so no need
  // to set `block_contents_pinned`.
  index_block.GetValue()->NewIndexIterator(
      internal_comparator()->user_comparator(),
      rep->get_global_seqno(BlockType::kIndex), &biter, kNullStats, true,
      index_has_first_key(), index_key_includes_seq(), index_value_is_full());
  // Index partitions are assumed to be consecuitive. Prefetch them all.
  // Read the first block offset
  biter.SeekToFirst();
  if (!biter.Valid()) {
    // Empty index.
    return biter.status();
  }
  handle = biter.value().handle;
  uint64_t prefetch_off = handle.offset();

  // Read the last block's offset
  biter.SeekToLast();
  if (!biter.Valid()) {
    // Empty index.
    return biter.status();
  }
  handle = biter.value().handle;
  uint64_t last_off =
      handle.offset() + BlockBasedTable::BlockSizeWithTrailer(handle);
  uint64_t prefetch_len = last_off - prefetch_off;
  std::unique_ptr<FilePrefetchBuffer> prefetch_buffer;
  rep->CreateFilePrefetchBuffer(
      0, 0, &prefetch_buffer, false /*Implicit auto readahead*/,
      0 /*num_reads_*/, 0 /*num_file_reads_for_auto_readahead*/);
  IOOptions opts;
  {
    Status s = rep->file->PrepareIOOptions(ro, opts);
    if (s.ok()) {
      s = prefetch_buffer->Prefetch(opts, rep->file.get(), prefetch_off,
                                    static_cast<size_t>(prefetch_len),
                                    ro.rate_limiter_priority);
    }
    if (!s.ok()) {
      return s;
    }
  }

  // For saving "all or nothing" to partition_map_
  UnorderedMap<uint64_t, CachableEntry<Block>> map_in_progress;

  // After prefetch, read the partitions one by one
  biter.SeekToFirst();
  size_t partition_count = 0;
  for (; biter.Valid(); biter.Next()) {
    handle = biter.value().handle;
    CachableEntry<Block> block;
    ++partition_count;
    // TODO: Support counter batch update for partitioned index and
    // filter blocks
    Status s = table()->MaybeReadBlockAndLoadToCache(
        prefetch_buffer.get(), ro, handle, UncompressionDict::GetEmptyDict(),
        /*wait=*/true, /*for_compaction=*/false, &block, BlockType::kIndex,
        /*get_context=*/nullptr, &lookup_context, /*contents=*/nullptr,
        /*async_read=*/false);

    if (!s.ok()) {
      return s;
    }
    if (block.GetValue() != nullptr) {
      // Might need to "pin" some mmap-read blocks (GetOwnValue) if some
      // partitions are successfully compressed (cached) and some are not
      // compressed (mmap eligible)
      if (block.IsCached() || block.GetOwnValue()) {
        if (pin) {
          map_in_progress[handle.offset()] = std::move(block);
        }
      }
    }
  }
  Status s = biter.status();
  // Save (pin) them only if everything checks out
  if (map_in_progress.size() == partition_count && s.ok()) {
    std::swap(partition_map_, map_in_progress);
  }
  return s;
}

}  // namespace ROCKSDB_NAMESPACE
