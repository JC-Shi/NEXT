#include "table/block_based/rtree_sec_index_iterator.h"
#include "logging/event_logger.h"
#include "logging/log_buffer.h"
#include "logging/logging.h"

#include <iostream>

namespace ROCKSDB_NAMESPACE {
void RtreeSecIndexIterator::Seek(const Slice& target) { 
  // std::cout << "RtreeSecIndexIterator Seek" << std::endl;
  SeekImpl(&target); 
}

void RtreeSecIndexIterator::SeekToFirst() { 
  // std::cout << "RtreeSecIndexIterator SeekToFirst" << std::endl;
  SeekImpl(nullptr); 
}

void RtreeSecIndexIterator::SeekImpl(const Slice* target) {
  SavePrevIndexValue();

  if (target != nullptr) {
    // The target for seek function here for secondary index shall be value
    query_mbr_ = ReadValueMbr(*target);
    std::cout << "target query_mbr_: " << query_mbr_ << std::endl;
  }

  // ================================================================================================

  // index_iter_->SeekToFirst();
  // ROCKS_LOG_DEBUG(table_->get_rep()->ioptions.logger, "top level index first entry mbr:  %s \n", ReadValueMbr(index_iter_->key()).toString().c_str());
  // ROCKS_LOG_DEBUG(table_->get_rep()->ioptions.logger, "Query mbr:  %s \n", query_mbr_.toString().c_str());
  // // std::cout << "top level index first entry mbr: " << ReadValueMbr(index_iter_->key()) << std::endl;
  // // std::cout << "query mbr: " << query_mbr_ << std::endl;
  // // std::cout << "index_iter_valid(): " << index_iter_->Valid() << " not intersect: " << (!IntersectMbrExcludeIID(ReadValueMbr(index_iter_->key()), query_mbr_)) << std::endl;
  // while (index_iter_->Valid() && !IntersectMbrExcludeIID(ReadValueMbr(index_iter_->key()), query_mbr_)) {
  //   // std::cout << "skipping top level index entry" << std::endl;
  //   index_iter_->Next();
  //   // std::cout << "next top level index mbr: " << ReadSecQueryMbr(index_iter_->key()) << std::endl;
  // }
  // if (rtree_height_ > 2) {
  //   // std::cout << "rtree_height > 2, rtree_height = " << rtree_height_ << std::endl;
  //   // if index_iter_ is Valid, and some of the next level node intersects with the query, add iterator to stack.
  //   if (index_iter_->Valid()) {
  //     // std::cout << "index_iter_->Valid()" << std::endl;
  //     AddChildToStack();
  //   }
  //   // std::cout << "iterator_stack size: " << iterator_stack_.size() << std::endl;
  //   // to deal with the case where the index_iter_ is still valid, but no child iterator added to stack
  //   while (iterator_stack_.empty() && index_iter_->Valid()) {
  //     // std::cout << "iterator_stack empty, advancing index_iter_" << std::endl;
  //     do {
  //       // std::cout << "skipping top level index entry" << std::endl;
  //       index_iter_->Next();
  //       // std::cout << "next top level index mbr: " << ReadValueMbr(index_iter_->key()) << std::endl;
  //     } while (index_iter_->Valid() && !IntersectMbrExcludeIID(ReadValueMbr(index_iter_->key()), query_mbr_));
  //     if (index_iter_->Valid()) {
  //       AddChildToStack();
  //     }
  //   }
  //   // std::cout << "iterator_stack_empty:" << !iterator_stack_.empty() << " iterator_stacK_top() level > 2: " << (iterator_stack_.top()->level > 2) << std::endl;
  //   while(!iterator_stack_.empty() && iterator_stack_.top()->level > 2){
  //     StackElement* current_top = iterator_stack_.top();
  //     if (!current_top->block_iter.Valid()) {
  //       iterator_stack_.pop();
  //     }
  //     else {
  //       AddChildToStack(current_top);
  //       RtreeIndexIterNext(&(current_top->block_iter));
  //     }
  //     while (iterator_stack_.empty() && index_iter_->Valid()) {
  //       do {
  //         // std::cout << "skipping top level index entry" << std::endl;
  //         index_iter_->Next();
  //         // std::cout << "next top level index mbr: " << ReadSecQueryMbr(index_iter_->key()) << std::endl;
  //       } while (index_iter_->Valid() && !IntersectMbrExcludeIID(ReadValueMbr(index_iter_->key()), query_mbr_));
  //       if (index_iter_->Valid()) {
  //         AddChildToStack();
  //       }   
  //     }
  //   }
  // }


  // if (!index_iter_->Valid()) {
  //   ResetPartitionedIndexIter();
  //   return;
  // }

  // // initiate block_iter_ on the sub-indexes
  // if (rtree_height_ <= 2) {
  //   InitPartitionedIndexBlock();
  // }
  // else {
  //   // std::cout << "rtree_sec_index_iterator.cc -> initpartionedindexblock" << std::endl;
  //   InitPartitionedIndexBlock(&(iterator_stack_.top()->block_iter));
  // }

  // ================================================================================================

  if (sec_blk_iter_ == found_sec_handles_->end()) {
    ResetPartitionedIndexIter();
    return;
  }

  
  InitPartitionedSecIndexBlock();
  // block_iter_mbr_.clear();

  // ================================================================================================

  block_iter_.SeekToFirst();
  // expandMbrExcludeIID(block_iter_mbr_, ReadValueMbr(block_iter_.key()));
  // std::cout << "block_iter_ mbr: " << ReadValueMbr(block_iter_.key()) << std::endl;
  while (block_iter_.Valid() && !IntersectMbrExcludeIID(ReadValueMbr(block_iter_.key()), query_mbr_)) {
    block_iter_.Next();
    // expandMbrExcludeIID(block_iter_mbr_, ReadValueMbr(block_iter_.key()));
  }

  // std::cout << "block_iter_ mbr: " << ReadValueMbr(block_iter_.key()) << std::endl;
  
  FindKeyForwardSec();
  // FindKeyForward();

  // std::cout << "First index entry value: " << ReadQueryMbr(block_iter_.key()) << std::endl;

  // TODO(PepperBun) seek for a specific secondary attribute target
  // if (target) {
  //   assert(!Valid() || (table_->get_rep()->index_key_includes_seq
  //                           ? (icomp_.Compare(*target, key()) <= 0)
  //                           : (user_comparator_.Compare(ExtractUserKey(*target),
  //                                                       key()) <= 0)));
  // }
}

void RtreeSecIndexIterator::SeekToLast() {
  // std::cout << "RtreeIndexIterator SeekToLast" << std::endl;
  SavePrevIndexValue();
  index_iter_->SeekToLast();
  if (!index_iter_->Valid()) {
    ResetPartitionedIndexIter();
    return;
  }
  InitPartitionedIndexBlock();
  block_iter_.SeekToLast();
  FindKeyBackward();
}

void RtreeSecIndexIterator::RtreeIndexIterSeekToFirst(IndexBlockIter* block_iter) {
  // std::cout << "RtreeIndexIterSeekToFirst" << std::endl;
  block_iter->SeekToFirst();
  while (block_iter->Valid() && !IntersectMbrExcludeIID(ReadValueMbr(block_iter->key()), query_mbr_)) {
    // std::cout << "skipping index entry" << std::endl;
    block_iter->Next();
    // std::cout << "next index mbr: " << ReadQueryMbr(block_iter->key()) << std::endl;
  }
}

void RtreeSecIndexIterator::RtreeIndexIterNext(IndexBlockIter* block_iter) {
  do {
    block_iter->Next();
  } while (block_iter->Valid() && !IntersectMbrExcludeIID(ReadValueMbr(block_iter->key()), query_mbr_));
}

void RtreeSecIndexIterator::Next() {
  // std::cout << "RtreeIndexIterator Next" << std::endl;
  assert(block_iter_points_to_real_block_);
  do {
    block_iter_.Next();
    // expandMbrExcludeIID(block_iter_mbr_, ReadValueMbr(block_iter_.key()));
    FindKeyForwardSec();
    // Mbr currentMbr = ReadQueryMbr(block_iter_.key());
    // std::cout << "current index entry: " << currentMbr << std::endl;
  } while (block_iter_.Valid() && !IntersectMbrExcludeIID(ReadValueMbr(block_iter_.key()), query_mbr_));
}

void RtreeSecIndexIterator::Prev() {
  assert(block_iter_points_to_real_block_);
  block_iter_.Prev();

  FindKeyBackward();
}

void RtreeSecIndexIterator::InitPartitionedIndexBlock(IndexBlockIter* block_iter) {
  BlockHandle partitioned_index_handle;
  if (block_iter == nullptr){
    partitioned_index_handle = index_iter_->value().handle;
  }
  else {
    partitioned_index_handle = block_iter->value().handle;
  }
  if (!block_iter_points_to_real_block_ ||
      partitioned_index_handle.offset() != prev_block_offset_ ||
      // if previous attempt of reading the block missed cache, try again
      block_iter_.status().IsIncomplete()) {
    if (block_iter_points_to_real_block_) {
      ResetPartitionedIndexIter();
    }
    auto* rep = table_->get_rep();
    bool is_for_compaction =
        lookup_context_.caller == TableReaderCaller::kCompaction;
    // Prefetch additional data for range scans (iterators).
    // Implicit auto readahead:
    //   Enabled after 2 sequential IOs when ReadOptions.readahead_size == 0.
    // Explicit user requested readahead:
    //   Enabled from the very first IO when ReadOptions.readahead_size is set.
    block_prefetcher_.PrefetchIfNeeded(
        rep, partitioned_index_handle, read_options_.readahead_size,
        is_for_compaction, /*no_sequential_checking=*/false,
        read_options_.rate_limiter_priority);
    Status s;
    table_->NewDataBlockIterator<IndexBlockIter>(
        read_options_, partitioned_index_handle, &block_iter_,
        BlockType::kIndex,
        /*get_context=*/nullptr, &lookup_context_,
        block_prefetcher_.prefetch_buffer(),
        /*for_compaction=*/is_for_compaction, /*async_read=*/false, s);
    block_iter_points_to_real_block_ = true;
    // We could check upper bound here but it is complicated to reason about
    // upper bound in index iterator. On the other than, in large scans, index
    // iterators are moved much less frequently compared to data blocks. So
    // the upper bound check is skipped for simplicity.
  }
}

void RtreeSecIndexIterator::InitPartitionedSecIndexBlock() {
  // std::cout << "InitPartitionedSecIndexBlock()" << std::endl;
  uint64_t pi_handle_offset = sec_blk_iter_->first;
  uint64_t pi_handle_size = sec_blk_iter_->second;
  BlockHandle partitioned_index_handle(pi_handle_offset, pi_handle_size);
  
  if (!block_iter_points_to_real_block_ ||
      partitioned_index_handle.offset() != prev_block_offset_ ||
      // if previous attempt of reading the block missed cache, try again
      block_iter_.status().IsIncomplete()) {
    if (block_iter_points_to_real_block_) {
      ResetPartitionedIndexIter();
    }
    auto* rep = table_->get_rep();
    bool is_for_compaction =
        lookup_context_.caller == TableReaderCaller::kCompaction;
    // Prefetch additional data for range scans (iterators).
    // Implicit auto readahead:
    //   Enabled after 2 sequential IOs when ReadOptions.readahead_size == 0.
    // Explicit user requested readahead:
    //   Enabled from the very first IO when ReadOptions.readahead_size is set.
    block_prefetcher_.PrefetchIfNeeded(
        rep, partitioned_index_handle, read_options_.readahead_size,
        is_for_compaction, /*no_sequential_checking=*/false,
        read_options_.rate_limiter_priority);
    Status s;
    table_->NewDataBlockIterator<IndexBlockIter>(
        read_options_, partitioned_index_handle, &block_iter_,
        BlockType::kIndex,
        /*get_context=*/nullptr, &lookup_context_,
        block_prefetcher_.prefetch_buffer(),
        /*for_compaction=*/is_for_compaction, /*async_read=*/false, s);
    block_iter_points_to_real_block_ = true;
    // We could check upper bound here but it is complicated to reason about
    // upper bound in index iterator. On the other than, in large scans, index
    // iterators are moved much less frequently compared to data blocks. So
    // the upper bound check is skipped for simplicity.
  }
}

void RtreeSecIndexIterator::InitRtreeIntermediateIndexBlock(IndexBlockIter* block_iter) {
  block_iter->Invalidate(Status::OK());
  BlockHandle partitioned_index_handle = index_iter_->value().handle;
  
    auto* rep = table_->get_rep();
    bool is_for_compaction =
        lookup_context_.caller == TableReaderCaller::kCompaction;
    block_prefetcher_.PrefetchIfNeeded(
        rep, partitioned_index_handle, read_options_.readahead_size,
        is_for_compaction, /*no_sequential_checking=*/false,
        read_options_.rate_limiter_priority);
    Status s;
    table_->NewDataBlockIterator<IndexBlockIter>(
        read_options_, partitioned_index_handle, block_iter,
        BlockType::kIndex,
        /*get_context=*/nullptr, &lookup_context_,
        block_prefetcher_.prefetch_buffer(),
        /*for_compaction=*/is_for_compaction, /*async_read=*/false, s);
}

void RtreeSecIndexIterator::InitRtreeIntermediateIndexBlock(IndexBlockIter* input_block_iter, IndexBlockIter* block_iter) {
  BlockHandle partitioned_index_handle = input_block_iter->value().handle;
  
    auto* rep = table_->get_rep();
    bool is_for_compaction =
        lookup_context_.caller == TableReaderCaller::kCompaction;
    block_prefetcher_.PrefetchIfNeeded(
        rep, partitioned_index_handle, read_options_.readahead_size,
        is_for_compaction, /*no_sequential_checking=*/false,
        read_options_.rate_limiter_priority);
    Status s;
    table_->NewDataBlockIterator<IndexBlockIter>(
        read_options_, partitioned_index_handle, block_iter,
        BlockType::kIndex,
        /*get_context=*/nullptr, &lookup_context_,
        block_prefetcher_.prefetch_buffer(),
        /*for_compaction=*/is_for_compaction, /*async_read=*/false, s);
}

void RtreeSecIndexIterator::AddChildToStack() {
  // std::cout << "AddChildToStack" << std::endl;
  StackElement* new_element = new StackElement;
  InitRtreeIntermediateIndexBlock(&(new_element->block_iter));
  RtreeIndexIterSeekToFirst(&(new_element->block_iter));
  if (new_element->block_iter.Valid()) {
    new_element->level = rtree_height_ - 1;
    iterator_stack_.push(new_element);
    // std::cout << "added iterator to stack, mbr: " << ReadValueMbr(new_element->block_iter.key()) << " level: " << new_element->level << std::endl;
  }
}

void RtreeSecIndexIterator::AddChildToStack(StackElement* current_top) {
  StackElement* new_element = new StackElement;
  InitRtreeIntermediateIndexBlock(&(current_top->block_iter), &(new_element->block_iter));
  RtreeIndexIterSeekToFirst(&(new_element->block_iter));
  if (new_element->block_iter.Valid()) {
    new_element->level = current_top->level - 1;
    iterator_stack_.push(new_element);
    // std:: cout << "added iterator to stack, mbr: " << ReadValueMbr(new_element->block_iter.key()) << "level: " << new_element->level << std::endl;
  }
}

void RtreeSecIndexIterator::FindKeyForward() {
  // This method's code is kept short to make it likely to be inlined.

  assert(block_iter_points_to_real_block_);

  if (!block_iter_.Valid()) {
    // This is the only call site of FindBlockForward(), but it's extracted into
    // a separate method to keep FindKeyForward() short and likely to be
    // inlined. When transitioning to a different block, we call
    // FindBlockForward(), which is much longer and is probably not inlined.
    FindBlockForward();
  } else {
    // This is the fast path that avoids a function call.
  }
}

void RtreeSecIndexIterator::FindKeyForwardSec() {
  // This method's code is kept short to make it likely to be inlined.

  assert(block_iter_points_to_real_block_);

  if (!block_iter_.Valid()) {
    // This is the only call site of FindBlockForward(), but it's extracted into
    // a separate method to keep FindKeyForward() short and likely to be
    // inlined. When transitioning to a different block, we call
    // FindBlockForward(), which is much longer and is probably not inlined.
    FindBlockForwardSec();
  } else {
    // This is the fast path that avoids a function call.
  }
}

void RtreeSecIndexIterator::FindBlockForwardSec() {
  // TODO the while loop inherits from two-level-iterator. We don't know
  // whether a block can be empty so it can be replaced by an "if".
  // std::cout << "RtreeIndexIteratorSec FindBlockForward" << std::endl;
  do {
    if (!block_iter_.status().ok()) {
      return;
    }
    ResetPartitionedIndexIter();

    if (sec_blk_iter_ == --found_sec_handles_->end()) {
      return;
    }

    sec_blk_iter_++;

    // std::cout << "Leaf block Mbr: " << block_iter_mbr_ << std::endl;

    InitPartitionedSecIndexBlock();
    // block_iter_mbr_.clear();
    
    block_iter_.SeekToFirst();
    // expandMbrExcludeIID(block_iter_mbr_, ReadValueMbr(block_iter_.key()));
    while (block_iter_.Valid() && !IntersectMbrExcludeIID(ReadValueMbr(block_iter_.key()), query_mbr_)) {
      block_iter_.Next();
      // expandMbrExcludeIID(block_iter_mbr_, ReadValueMbr(block_iter_.key()));
    }

    // std::cout << "block_iter_ MBR: " << ReadValueMbr(block_iter_.key()) << std::endl;
  } while (!block_iter_.Valid());
}

void RtreeSecIndexIterator::FindBlockForward() {
  // TODO the while loop inherits from two-level-iterator. We don't know
  // whether a block can be empty so it can be replaced by an "if".
  // std::cout << "RtreeIndexIterator FindBlockForward" << std::endl;
  do {
    if (!block_iter_.status().ok()) {
      return;
    }
    ResetPartitionedIndexIter();
    if (rtree_height_ == 2) {
      do {
        index_iter_->Next();
        // std::cout << "next top level index mbr: " << ReadValueMbr(index_iter_->key()) << std::endl;
      } while (index_iter_->Valid() && !IntersectMbrExcludeIID(ReadValueMbr(index_iter_->key()), query_mbr_));
    }

    if (rtree_height_ > 2) {
      StackElement* current_top = iterator_stack_.top();
      RtreeIndexIterNext(&(current_top->block_iter));
      if (!current_top->block_iter.Valid()) {
        iterator_stack_.pop();
      }
      // if iterator_stack is empty, advance index_iter_ and add iterator to stack
      // to deal with the case where the index_iter_ is still valid, but no child iterator added to stack
      while (iterator_stack_.empty() && index_iter_->Valid()) {
        // std::cout << "iterator_stack empty, advancing index_iter_" << std::endl;
        do {
          // std::cout << "skipping top level index entry" << std::endl;
          index_iter_->Next();
          // std::cout << "next top level index mbr: " << ReadQueryMbr(index_iter_->key()) << std::endl;
        } while (index_iter_->Valid() && !IntersectMbrExcludeIID(ReadValueMbr(index_iter_->key()), query_mbr_));
        if (index_iter_->Valid()) {
          AddChildToStack();
        }
      }
      while(!iterator_stack_.empty() && iterator_stack_.top()->level > 2){
        current_top = iterator_stack_.top();
        if (!current_top->block_iter.Valid()) {
          iterator_stack_.pop();
        }
        else {
          AddChildToStack(current_top);
          RtreeIndexIterNext(&(current_top->block_iter));
        }
        while (iterator_stack_.empty() && index_iter_->Valid()) {
          do {
            // std::cout << "skipping top level index entry" << std::endl;
            index_iter_->Next();
            // std::cout << "next top level index mbr: " << ReadQueryMbr(index_iter_->key()) << std::endl;
          } while (index_iter_->Valid() && !IntersectMbrExcludeIID(ReadValueMbr(index_iter_->key()), query_mbr_));
          if (index_iter_->Valid()) {
            AddChildToStack();
          }   
        }
      }   
    }

    if (!index_iter_->Valid()) {
      return;
    }

    if (rtree_height_ <= 2) {
      InitPartitionedIndexBlock();
    }
    else {
      InitPartitionedIndexBlock(&(iterator_stack_.top()->block_iter));
      // std::cout << "iterator MBR: " << ReadQueryMbr(iterator_stack_.top()->block_iter.key()) << std::endl;
    }
    block_iter_.SeekToFirst();
    // std::cout << "block_iter_ MBR: " << ReadValueMbr(block_iter_.key()) << std::endl;
  } while (!block_iter_.Valid());
}

void RtreeSecIndexIterator::FindKeyBackward() {
  while (!block_iter_.Valid()) {
    if (!block_iter_.status().ok()) {
      return;
    }

    ResetPartitionedIndexIter();
    index_iter_->Prev();

    if (index_iter_->Valid()) {
      InitPartitionedIndexBlock();
      block_iter_.SeekToLast();
    } else {
      return;
    }
  }
}
}  // namespace ROCKSDB_NAMESPACE
