//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/block_based/index_builder.h"

#include <assert.h>

#include <cinttypes>
#include <list>
#include <string>

#include "db/dbformat.h"
#include "rocksdb/comparator.h"
#include "rocksdb/flush_block_policy.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/block_based/partitioned_filter_block.h"
#include "table/format.h"
#include "util/rtree.h"
#include "util/z_curve.h"

namespace ROCKSDB_NAMESPACE {

// Create a index builder based on its type.
IndexBuilder* IndexBuilder::CreateIndexBuilder(
    BlockBasedTableOptions::IndexType index_type,
    const InternalKeyComparator* comparator,
    const InternalKeySliceTransform* int_key_slice_transform,
    const bool use_value_delta_encoding,
    const BlockBasedTableOptions& table_opt) {
  IndexBuilder* result = nullptr;
  switch (index_type) {
    case BlockBasedTableOptions::kBinarySearch: {
      result = new ShortenedIndexBuilder(
          comparator, table_opt.index_block_restart_interval,
          table_opt.format_version, use_value_delta_encoding,
          table_opt.index_shortening, /* include_first_key */ false);
      break;
    }
    case BlockBasedTableOptions::kHashSearch: {
      // Currently kHashSearch is incompatible with index_block_restart_interval
      // > 1
      assert(table_opt.index_block_restart_interval == 1);
      result = new HashIndexBuilder(
          comparator, int_key_slice_transform,
          table_opt.index_block_restart_interval, table_opt.format_version,
          use_value_delta_encoding, table_opt.index_shortening);
      break;
    }
    case BlockBasedTableOptions::kTwoLevelIndexSearch: {
      result = PartitionedIndexBuilder::CreateIndexBuilder(
          comparator, use_value_delta_encoding, table_opt);
      break;
    }
    case BlockBasedTableOptions::kBinarySearchWithFirstKey: {
      result = new ShortenedIndexBuilder(
          comparator, table_opt.index_block_restart_interval,
          table_opt.format_version, use_value_delta_encoding,
          table_opt.index_shortening, /* include_first_key */ true);
      break;
    }
    case BlockBasedTableOptions::kRtreeSearch: {
      // result = new RtreeIndexLevelBuilder(
      //     comparator, table_opt.index_block_restart_interval,
      //     table_opt.format_version, use_value_delta_encoding,
      //     table_opt.index_shortening, /* include_first_key */ false);
      result = RtreeIndexBuilder::CreateIndexBuilder(
          comparator, use_value_delta_encoding, table_opt);
      // result = PartitionedIndexBuilder::CreateIndexBuilder(
      //     comparator, use_value_delta_encoding, table_opt);
      break;
    }
    default: {
      assert(!"Do not recognize the index type ");
      break;
    }
  }
  return result;
}

// Create a Secondary index builder based on its type.
SecondaryIndexBuilder* SecondaryIndexBuilder::CreateSecIndexBuilder(
    BlockBasedTableOptions::SecondaryIndexType sec_index_type,
    const InternalKeyComparator* comparator,
    const InternalKeySliceTransform* int_key_slice_transform,
    const bool use_value_delta_encoding,
    const BlockBasedTableOptions& table_opt) {
  (void) int_key_slice_transform;
  SecondaryIndexBuilder* result = nullptr;
  switch (sec_index_type) {
    case BlockBasedTableOptions::kRtreeSec: {
      result = RtreeSecondaryIndexBuilder::CreateIndexBuilder(
          comparator, use_value_delta_encoding, table_opt);
      break;
    }
    case BlockBasedTableOptions::kOneDRtreeSec: {
      result = OneDRtreeSecondaryIndexBuilder::CreateIndexBuilder(
         comparator, use_value_delta_encoding, table_opt);
      break;
    }
    default: {
      assert(!"Do not recognize the index type ");
      break;
    }
  }
  return result;
}

void ShortenedIndexBuilder::FindShortestInternalKeySeparator(
    const Comparator& comparator, std::string* start, const Slice& limit) {
  // Attempt to shorten the user portion of the key
  Slice user_start = ExtractUserKey(*start);
  Slice user_limit = ExtractUserKey(limit);
  std::string tmp(user_start.data(), user_start.size());
  comparator.FindShortestSeparator(&tmp, user_limit);
  if (tmp.size() <= user_start.size() &&
      comparator.Compare(user_start, tmp) < 0) {
    // User key has become shorter physically, but larger logically.
    // Tack on the earliest possible number to the shortened user key.
    PutFixed64(&tmp,
               PackSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek));
    assert(InternalKeyComparator(&comparator).Compare(*start, tmp) < 0);
    assert(InternalKeyComparator(&comparator).Compare(tmp, limit) < 0);
    start->swap(tmp);
  }
}

void ShortenedIndexBuilder::FindShortInternalKeySuccessor(
    const Comparator& comparator, std::string* key) {
  Slice user_key = ExtractUserKey(*key);
  std::string tmp(user_key.data(), user_key.size());
  comparator.FindShortSuccessor(&tmp);
  if (tmp.size() <= user_key.size() && comparator.Compare(user_key, tmp) < 0) {
    // User key has become shorter physically, but larger logically.
    // Tack on the earliest possible number to the shortened user key.
    PutFixed64(&tmp,
               PackSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek));
    assert(InternalKeyComparator(&comparator).Compare(*key, tmp) < 0);
    key->swap(tmp);
  }
}

PartitionedIndexBuilder* PartitionedIndexBuilder::CreateIndexBuilder(
    const InternalKeyComparator* comparator,
    const bool use_value_delta_encoding,
    const BlockBasedTableOptions& table_opt) {
  return new PartitionedIndexBuilder(comparator, table_opt,
                                     use_value_delta_encoding);
}

PartitionedIndexBuilder::PartitionedIndexBuilder(
    const InternalKeyComparator* comparator,
    const BlockBasedTableOptions& table_opt,
    const bool use_value_delta_encoding)
    : IndexBuilder(comparator),
      index_block_builder_(table_opt.index_block_restart_interval,
                           true /*use_delta_encoding*/,
                           use_value_delta_encoding),
      index_block_builder_without_seq_(table_opt.index_block_restart_interval,
                                       true /*use_delta_encoding*/,
                                       use_value_delta_encoding),
      sub_index_builder_(nullptr),
      table_opt_(table_opt),
      // We start by false. After each partition we revise the value based on
      // what the sub_index_builder has decided. If the feature is disabled
      // entirely, this will be set to true after switching the first
      // sub_index_builder. Otherwise, it could be set to true even one of the
      // sub_index_builders could not safely exclude seq from the keys, then it
      // wil be enforced on all sub_index_builders on ::Finish.
      seperator_is_key_plus_seq_(false),
      use_value_delta_encoding_(use_value_delta_encoding) {}

PartitionedIndexBuilder::~PartitionedIndexBuilder() {
  delete sub_index_builder_;
}

void PartitionedIndexBuilder::MakeNewSubIndexBuilder() {
  assert(sub_index_builder_ == nullptr);
  sub_index_builder_ = new ShortenedIndexBuilder(
      comparator_, table_opt_.index_block_restart_interval,
      table_opt_.format_version, use_value_delta_encoding_,
      table_opt_.index_shortening, /* include_first_key */ false);

  // Set sub_index_builder_->seperator_is_key_plus_seq_ to true if
  // seperator_is_key_plus_seq_ is true (internal-key mode) (set to false by
  // default on Creation) so that flush policy can point to
  // sub_index_builder_->index_block_builder_
  if (seperator_is_key_plus_seq_) {
    sub_index_builder_->seperator_is_key_plus_seq_ = true;
  }

  flush_policy_.reset(FlushBlockBySizePolicyFactory::NewFlushBlockPolicy(
      table_opt_.metadata_block_size, table_opt_.block_size_deviation,
      // Note: this is sub-optimal since sub_index_builder_ could later reset
      // seperator_is_key_plus_seq_ but the probability of that is low.
      sub_index_builder_->seperator_is_key_plus_seq_
          ? sub_index_builder_->index_block_builder_
          : sub_index_builder_->index_block_builder_without_seq_));
  partition_cut_requested_ = false;
}

void PartitionedIndexBuilder::RequestPartitionCut() {
  partition_cut_requested_ = true;
}

void PartitionedIndexBuilder::AddIndexEntry(
    std::string* last_key_in_current_block,
    const Slice* first_key_in_next_block, const BlockHandle& block_handle) {
  // Note: to avoid two consecuitive flush in the same method call, we do not
  // check flush policy when adding the last key
  if (UNLIKELY(first_key_in_next_block == nullptr)) {  // no more keys
    if (sub_index_builder_ == nullptr) {
      MakeNewSubIndexBuilder();
    }
    sub_index_builder_->AddIndexEntry(last_key_in_current_block,
                                      first_key_in_next_block, block_handle);
    if (!seperator_is_key_plus_seq_ &&
        sub_index_builder_->seperator_is_key_plus_seq_) {
      // then we need to apply it to all sub-index builders and reset
      // flush_policy to point to Block Builder of sub_index_builder_ that store
      // internal keys.
      seperator_is_key_plus_seq_ = true;
      flush_policy_.reset(FlushBlockBySizePolicyFactory::NewFlushBlockPolicy(
          table_opt_.metadata_block_size, table_opt_.block_size_deviation,
          sub_index_builder_->index_block_builder_));
    }
    sub_index_last_key_ = std::string(*last_key_in_current_block);
    entries_.push_back(
        {sub_index_last_key_,
         std::unique_ptr<ShortenedIndexBuilder>(sub_index_builder_)});
    sub_index_builder_ = nullptr;
    cut_filter_block = true;
  } else {
    // apply flush policy only to non-empty sub_index_builder_
    if (sub_index_builder_ != nullptr) {
      std::string handle_encoding;
      block_handle.EncodeTo(&handle_encoding);
      bool do_flush =
          partition_cut_requested_ ||
          flush_policy_->Update(*last_key_in_current_block, handle_encoding);
      if (do_flush) {
        entries_.push_back(
            {sub_index_last_key_,
             std::unique_ptr<ShortenedIndexBuilder>(sub_index_builder_)});
        cut_filter_block = true;
        sub_index_builder_ = nullptr;
      }
    }
    if (sub_index_builder_ == nullptr) {
      MakeNewSubIndexBuilder();
    }
    sub_index_builder_->AddIndexEntry(last_key_in_current_block,
                                      first_key_in_next_block, block_handle);
    sub_index_last_key_ = std::string(*last_key_in_current_block);
    if (!seperator_is_key_plus_seq_ &&
        sub_index_builder_->seperator_is_key_plus_seq_) {
      // then we need to apply it to all sub-index builders and reset
      // flush_policy to point to Block Builder of sub_index_builder_ that store
      // internal keys.
      seperator_is_key_plus_seq_ = true;
      flush_policy_.reset(FlushBlockBySizePolicyFactory::NewFlushBlockPolicy(
          table_opt_.metadata_block_size, table_opt_.block_size_deviation,
          sub_index_builder_->index_block_builder_));
    }
  }
}

Status PartitionedIndexBuilder::Finish(
    IndexBlocks* index_blocks, const BlockHandle& last_partition_block_handle) {
  if (partition_cnt_ == 0) {
    partition_cnt_ = entries_.size();
  }
  // It must be set to null after last key is added
  assert(sub_index_builder_ == nullptr);
  if (finishing_indexes == true) {
    Entry& last_entry = entries_.front();
    std::string handle_encoding;
    last_partition_block_handle.EncodeTo(&handle_encoding);
    std::string handle_delta_encoding;
    PutVarsignedint64(
        &handle_delta_encoding,
        last_partition_block_handle.size() - last_encoded_handle_.size());
    last_encoded_handle_ = last_partition_block_handle;
    const Slice handle_delta_encoding_slice(handle_delta_encoding);
    index_block_builder_.Add(last_entry.key, handle_encoding,
                             &handle_delta_encoding_slice);
    if (!seperator_is_key_plus_seq_) {
      index_block_builder_without_seq_.Add(ExtractUserKey(last_entry.key),
                                           handle_encoding,
                                           &handle_delta_encoding_slice);
    }
    entries_.pop_front();
  }
  // If there is no sub_index left, then return the 2nd level index.
  if (UNLIKELY(entries_.empty())) {
    if (seperator_is_key_plus_seq_) {
      index_blocks->index_block_contents = index_block_builder_.Finish();
    } else {
      index_blocks->index_block_contents =
          index_block_builder_without_seq_.Finish();
    }
    top_level_index_size_ = index_blocks->index_block_contents.size();
    index_size_ += top_level_index_size_;
    return Status::OK();
  } else {
    // Finish the next partition index in line and Incomplete() to indicate we
    // expect more calls to Finish
    Entry& entry = entries_.front();
    // Apply the policy to all sub-indexes
    entry.value->seperator_is_key_plus_seq_ = seperator_is_key_plus_seq_;
    auto s = entry.value->Finish(index_blocks);
    index_size_ += index_blocks->index_block_contents.size();
    finishing_indexes = true;
    return s.ok() ? Status::Incomplete() : s;
  }
}

size_t PartitionedIndexBuilder::NumPartitions() const { return partition_cnt_; }


RtreeIndexBuilder* RtreeIndexBuilder::CreateIndexBuilder(
    const InternalKeyComparator* comparator,
    const bool use_value_delta_encoding,
    const BlockBasedTableOptions& table_opt) {
  return new RtreeIndexBuilder(comparator, table_opt,
                                     use_value_delta_encoding);
}

RtreeIndexBuilder::RtreeIndexBuilder(
    const InternalKeyComparator* comparator,
    const BlockBasedTableOptions& table_opt,
    const bool use_value_delta_encoding)
    : IndexBuilder(comparator),
      index_block_builder_(table_opt.index_block_restart_interval,
                           true /*use_delta_encoding*/,
                           use_value_delta_encoding),
      sub_index_builder_(nullptr),
      table_opt_(table_opt),
      use_value_delta_encoding_(use_value_delta_encoding),
      rtree_level_(1) {}

RtreeIndexBuilder::~RtreeIndexBuilder() {
  delete sub_index_builder_;
}

void RtreeIndexBuilder::MakeNewSubIndexBuilder() {
  assert(sub_index_builder_ == nullptr);
  sub_index_builder_ = new RtreeIndexLevelBuilder(
      comparator_, table_opt_.index_block_restart_interval,
      table_opt_.format_version, use_value_delta_encoding_,
      table_opt_.index_shortening, /* include_first_key */ false);

  flush_policy_.reset(FlushBlockBySizePolicyFactory::NewFlushBlockPolicy(
      table_opt_.metadata_block_size, table_opt_.block_size_deviation,
      sub_index_builder_->index_block_builder_));
  partition_cut_requested_ = false;
}

void RtreeIndexBuilder::RequestPartitionCut() {
  partition_cut_requested_ = true;
}

void RtreeIndexBuilder::OnKeyAdded(const Slice& key){
    Slice key_temp = Slice(key);
    Mbr mbr = ReadKeyMbr(key_temp);
    expandMbr(sub_index_enclosing_mbr_, mbr);
    // std::cout << "sub_index_enclosing_mbr_ after expansion: " << sub_index_enclosing_mbr_ << std::endl;
  }

void RtreeIndexBuilder::AddIndexEntry(
    std::string* last_key_in_current_block,
    const Slice* first_key_in_next_block, const BlockHandle& block_handle) {
  expandMbr(enclosing_mbr_, sub_index_enclosing_mbr_);
  // std::cout << "enclosing_mbr_: " << enclosing_mbr_ << std::endl;
  // Note: to avoid two consecuitive flush in the same method call, we do not
  // check flush policy when adding the last key
  if (UNLIKELY(first_key_in_next_block == nullptr)) {  // no more keys
    if (sub_index_builder_ == nullptr) {
      MakeNewSubIndexBuilder();
    }
    sub_index_builder_->AddIndexEntry(block_handle, serializeMbr(sub_index_enclosing_mbr_));

    sub_index_last_key_ = std::string(*last_key_in_current_block);
    // std::cout << "push_back the last sub_index builder" << std::endl;
    entries_.push_back(
        {serializeMbr(enclosing_mbr_),
         std::unique_ptr<RtreeIndexLevelBuilder>(sub_index_builder_)});
    enclosing_mbr_.clear();
    sub_index_builder_ = nullptr;
    cut_filter_block = true;
  } else {
    // apply flush policy only to non-empty sub_index_builder_
    if (sub_index_builder_ != nullptr) {
      std::string handle_encoding;
      block_handle.EncodeTo(&handle_encoding);
      bool do_flush =
          partition_cut_requested_ ||
          flush_policy_->Update(*last_key_in_current_block, handle_encoding);
      if (do_flush) {
        // std::cout << "push_back a full sub_index builder" << std::endl;
        entries_.push_back(
            {serializeMbr(enclosing_mbr_),
             std::unique_ptr<RtreeIndexLevelBuilder>(sub_index_builder_)});
        enclosing_mbr_.clear();
        sub_index_builder_ = nullptr;
      }
    }
    if (sub_index_builder_ == nullptr) {
      MakeNewSubIndexBuilder();
    }
    sub_index_builder_->AddIndexEntry(block_handle, serializeMbr(sub_index_enclosing_mbr_));
    sub_index_last_key_ = std::string(*last_key_in_current_block);

  }
  sub_index_enclosing_mbr_.clear();
}

Status RtreeIndexBuilder::Finish(
    IndexBlocks* index_blocks, const BlockHandle& last_partition_block_handle) {
  // std::cout << "RtreeIndexBuilder Finish" << std::endl;
  // std::cout << "entries_ size: " << entries_.size() << std::endl;
  if (partition_cnt_ == 0) {
    partition_cnt_ = entries_.size();
  }
  // It must be set to null after last key is added
  // assert(sub_index_builder_ == nullptr);
  if (finishing_indexes == true) {
    Entry& last_entry = entries_.front();
    // std::string handle_encoding;
    // last_partition_block_handle.EncodeTo(&handle_encoding);
    // std::string handle_delta_encoding;
    // PutVarsignedint64(
    //     &handle_delta_encoding,
    //     last_partition_block_handle.size() - last_encoded_handle_.size());
    // last_encoded_handle_ = last_partition_block_handle;
    // const Slice handle_delta_encoding_slice(handle_delta_encoding);
    // // std::cout << "add index entry into top level index: " << ReadQueryMbr(last_entry.key) << std::endl;
    // index_block_builder_.Add(last_entry.key, handle_encoding,
    //                          &handle_delta_encoding_slice);

    // add entry to the next_sub_index_builder
    if (sub_index_builder_ != nullptr) {
      std::string handle_encoding;
      last_partition_block_handle.EncodeTo(&handle_encoding);
      bool do_flush =
          partition_cut_requested_ ||
          flush_policy_->Update(last_entry.key, handle_encoding);
      if (do_flush) {
        // std::cout << "enclosing_mbr: " << enclosing_mbr_ << std::endl;
        next_level_entries_.push_back(
            {serializeMbr(enclosing_mbr_),
             std::unique_ptr<RtreeIndexLevelBuilder>(sub_index_builder_)});
        enclosing_mbr_.clear();
        sub_index_builder_ = nullptr;
      }
    }
    if (sub_index_builder_ == nullptr) {
      MakeNewSubIndexBuilder();
    }
    sub_index_builder_->AddIndexEntry(last_partition_block_handle, last_entry.key);
    expandMbr(enclosing_mbr_, ReadQueryMbr(last_entry.key));

    entries_.pop_front();
  }
  // If the current R-tree level has been constructed, move on to the next level.
  if (UNLIKELY(entries_.empty())) {

    // update R-tree height
    rtree_level_++;

    if (sub_index_builder_ != nullptr){
      next_level_entries_.push_back(
          {serializeMbr(enclosing_mbr_),
          std::unique_ptr<RtreeIndexLevelBuilder>(sub_index_builder_)});
      enclosing_mbr_.clear();
      sub_index_builder_ = nullptr;
    }

    // return if current level only contains one block
    // std::cout << "next_level_entries_ size: " << next_level_entries_.size() << std::endl;
    if (next_level_entries_.size() == 1) {
      Entry& entry = next_level_entries_.front();
      auto s = entry.value->Finish(index_blocks);
      // std::cout << "writing the top-level index block with enclosing MBR: " << ReadQueryMbr(entry.key) << std::endl;
      index_size_ += index_blocks->index_block_contents.size();
      PutVarint32(&rtree_height_str_, rtree_level_);
      index_blocks->meta_blocks.insert(
        {kRtreeIndexMetadataBlock.c_str(), rtree_height_str_});
      // std::cout << "R-tree height: " << rtree_level_ << std::endl;
      return s;
    }

    // swaping the contents of entries_ and next_level_entries
    for (std::list<Entry>::iterator it = next_level_entries_.begin(), end = next_level_entries_.end(); it != end; ++it) {
      entries_.push_back({it->key, std::move(it->value)});
      // std::cout << "add new item to entries: " << ReadQueryMbr(it->key) << std::endl;
    }

    Entry& entry = entries_.front();
    auto s = entry.value->Finish(index_blocks);
    // std::cout << "writing an index block to disk with enclosing MBR: " << ReadQueryMbr(entry.key) << std::endl;
    index_size_ += index_blocks->index_block_contents.size();
    finishing_indexes = true;

    next_level_entries_.clear();
    // std::cout << "swapped next_level_entries and entries_, entries size: " << entries_.size() << std::endl;

    return Status::Incomplete();

    // index_blocks->index_block_contents = index_block_builder_.Finish();

    // top_level_index_size_ = index_blocks->index_block_contents.size();
    // index_size_ += top_level_index_size_;
    // return Status::OK();
  } else {
    // Finish the next partition index in line and Incomplete() to indicate we
    // expect more calls to Finish
    Entry& entry = entries_.front();

    auto s = entry.value->Finish(index_blocks);
    // std::cout << "writing an index block to disk with enclosing MBR: " << ReadQueryMbr(entry.key) << std::endl;
    index_size_ += index_blocks->index_block_contents.size();
    finishing_indexes = true;
    return s.ok() ? Status::Incomplete() : s;
  }
}

size_t RtreeIndexBuilder::NumPartitions() const { return partition_cnt_; }

// void RtreeIndexLevelBuilder::FindShortestInternalKeySeparator(
//     const Comparator& comparator, std::string* start, const Slice& limit) {
//   // Attempt to shorten the user portion of the key
//   Slice user_start = ExtractUserKey(*start);
//   Slice user_limit = ExtractUserKey(limit);
//   std::string tmp(user_start.data(), user_start.size());
//   comparator.FindShortestSeparator(&tmp, user_limit);
//   if (tmp.size() <= user_start.size() &&
//       comparator.Compare(user_start, tmp) < 0) {
//     // User key has become shorter physically, but larger logically.
//     // Tack on the earliest possible number to the shortened user key.
//     PutFixed64(&tmp,
//                PackSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek));
//     assert(InternalKeyComparator(&comparator).Compare(*start, tmp) < 0);
//     assert(InternalKeyComparator(&comparator).Compare(tmp, limit) < 0);
//     start->swap(tmp);
//   }
// }

// void RtreeIndexLevelBuilder::FindShortInternalKeySuccessor(
//     const Comparator& comparator, std::string* key) {
//   Slice user_key = ExtractUserKey(*key);
//   std::string tmp(user_key.data(), user_key.size());
//   comparator.FindShortSuccessor(&tmp);
//   if (tmp.size() <= user_key.size() && comparator.Compare(user_key, tmp) < 0) {
//     // User key has become shorter physically, but larger logically.
//     // Tack on the earliest possible number to the shortened user key.
//     PutFixed64(&tmp,
//                PackSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek));
//     assert(InternalKeyComparator(&comparator).Compare(*key, tmp) < 0);
//     key->swap(tmp);
//   }
// }

RtreeSecondaryIndexBuilder* RtreeSecondaryIndexBuilder::CreateIndexBuilder(
    const InternalKeyComparator* comparator,
    const bool use_value_delta_encoding,
    const BlockBasedTableOptions& table_opt) {
  return new RtreeSecondaryIndexBuilder(comparator, table_opt,
                                     use_value_delta_encoding);
}

RtreeSecondaryIndexBuilder::RtreeSecondaryIndexBuilder(
    const InternalKeyComparator* comparator,
    const BlockBasedTableOptions& table_opt,
    const bool use_value_delta_encoding)
    : SecondaryIndexBuilder(comparator),
      index_block_builder_(table_opt.index_block_restart_interval,
                           true /*use_delta_encoding*/,
                           use_value_delta_encoding),
      sub_index_builder_(nullptr),
      table_opt_(table_opt),
      use_value_delta_encoding_(use_value_delta_encoding),
      rtree_level_(1) {}

RtreeSecondaryIndexBuilder::~RtreeSecondaryIndexBuilder() {
  delete sub_index_builder_;
}

void RtreeSecondaryIndexBuilder::MakeNewSubIndexBuilder() {
  assert(sub_index_builder_ == nullptr);
  sub_index_builder_ = new RtreeSecondaryIndexLevelBuilder(
      comparator_, table_opt_.index_block_restart_interval,
      table_opt_.format_version, use_value_delta_encoding_,
      table_opt_.index_shortening, /* include_first_key */ false);

  flush_policy_.reset(FlushBlockBySizePolicyFactory::NewFlushBlockPolicy(
      table_opt_.metadata_block_size, table_opt_.block_size_deviation,
      sub_index_builder_->index_block_builder_));
  partition_cut_requested_ = false;
}

void RtreeSecondaryIndexBuilder::RequestPartitionCut() {
  partition_cut_requested_ = true;
}

void RtreeSecondaryIndexBuilder::OnKeyAdded(const Slice& value){
    Slice val_temp = Slice(value);
    Mbr mbr = ReadValueMbr(val_temp);
    tuple_mbrs_.push_back(mbr);
    // expandMbrExcludeIID(sub_index_enclosing_mbr_, mbr);
  }

void RtreeSecondaryIndexBuilder::AddIndexEntry(
    std::string* last_key_in_current_block,
    const Slice* first_key_in_next_block, const BlockHandle& block_handle) {
  // expandMbrExcludeIID(enclosing_mbr_, sub_index_enclosing_mbr_);
  (void) first_key_in_next_block;
  std::string datablocklastkeystr = std::string(*last_key_in_current_block);
  for (const Mbr& m :tuple_mbrs_) {
    DataBlockEntry dbe;
    dbe.datablockhandle = block_handle;
    dbe.datablocklastkey = datablocklastkeystr;
    dbe.subindexenclosingmbr = serializeMbrExcludeIID(m);
    data_block_entries_.push_back(dbe);
  }
  tuple_mbrs_.clear();
  
  // dbe.subindexenclosingmbr = serializeMbrExcludeIID(sub_index_enclosing_mbr_);
  // data_block_entries_.push_back(dbe);
  // sub_index_enclosing_mbr_.clear();    

}

void RtreeSecondaryIndexBuilder::AddIdxEntry(DataBlockEntry datablkentry, bool last) {
  expandMbrExcludeIID(enclosing_mbr_, ReadSecQueryMbr(datablkentry.subindexenclosingmbr));
  // std::cout << "enclosing_mbr_: " << enclosing_mbr_ << std::endl;
  // Note: to avoid two consecuitive flush in the same method call, we do not
  // check flush policy when adding the last key
  if (UNLIKELY(last == true)) {  // no more keys
    if (sub_index_builder_ == nullptr) {
      MakeNewSubIndexBuilder();
    }
    sub_index_builder_->AddIndexEntry(datablkentry.datablockhandle, datablkentry.subindexenclosingmbr);

    // std::cout << "pushed mbr: " << enclosing_mbr_ << std::endl;
    sub_index_last_key_ = serializeMbrExcludeIID(enclosing_mbr_);
    // std::cout << "push_back the last sub_index builder" << std::endl;
    entries_.push_back(
        {serializeMbrExcludeIID(enclosing_mbr_),
         std::unique_ptr<RtreeSecondaryIndexLevelBuilder>(sub_index_builder_)});
    enclosing_mbr_.clear();
    sub_index_builder_ = nullptr;
    cut_filter_block = true;
  } else {
    // apply flush policy only to non-empty sub_index_builder_
    if (sub_index_builder_ != nullptr) {
      std::string handle_encoding;
      std::string enclosing_mbr_encoding;
      enclosing_mbr_encoding = serializeMbrExcludeIID(enclosing_mbr_);
      datablkentry.datablockhandle.EncodeTo(&handle_encoding);
      bool do_flush =
          partition_cut_requested_ ||
          flush_policy_->Update(enclosing_mbr_encoding, handle_encoding);
      if (do_flush) {
        // std::cout << "push_back a full sub_index builder" << std::endl;
        // std::cout << "pushed mbr: " << enclosing_mbr_ << std::endl;
        entries_.push_back(
            {serializeMbrExcludeIID(enclosing_mbr_),
             std::unique_ptr<RtreeSecondaryIndexLevelBuilder>(sub_index_builder_)});
        enclosing_mbr_.clear();
        sub_index_builder_ = nullptr;
      }
    }
    if (sub_index_builder_ == nullptr) {
      MakeNewSubIndexBuilder();
    }
    sub_index_builder_->AddIndexEntry(datablkentry.datablockhandle, datablkentry.subindexenclosingmbr);
    sub_index_last_key_ = serializeMbrExcludeIID(enclosing_mbr_);

  }
}

Status RtreeSecondaryIndexBuilder::Finish(
    IndexBlocks* index_blocks, const BlockHandle& last_partition_block_handle) {

  if (finishing_indexes == false) {
    
    data_block_entries_.sort([](const DataBlockEntry& a, const DataBlockEntry& b){
      Mbr a_mbr = ReadSecQueryMbr(a.subindexenclosingmbr);
      Mbr b_mbr = ReadSecQueryMbr(b.subindexenclosingmbr);

      // defining z-curve based on grid cells on data space
      // for dataset india
      // double x_min = -12.2304942;
      // double x_max = 37.4497039;
      // double y_min = 50.0218541;
      // double y_max = 125.9548288;
      // for dataset buildings
      double x_min = -179.7582827;
      double x_max = 179.8440789;
      double y_min = -89.9678358;
      double y_max = 82.5114551;
      int n = 8192;

      // using the centre point of each mbr for z-value computation
      double x_a = (a_mbr.first.min + a_mbr.first.max) / 2;
      double y_a = (a_mbr.second.min + a_mbr.second.max) / 2;
      double x_b = (b_mbr.first.min + b_mbr.first.max) / 2;
      double y_b = (b_mbr.second.min + b_mbr.second.max) / 2;
      // double x_a = a_mbr.first.min;
      // double y_a = a_mbr.second.min;
      // double x_b = b_mbr.first.min;
      // double y_b = b_mbr.second.min;

      // compute the z-values
      uint32_t x_a_int = std::min(int(floor((x_a - x_min)  / ((x_max - x_min) / n))), n-1);
      uint32_t y_a_int = std::min(int(floor((y_a - y_min)  / ((y_max - y_min) / n))), n-1);
      uint32_t x_b_int = std::min(int(floor((x_b - x_min)  / ((x_max - x_min) / n))), n-1);
      uint32_t y_b_int = std::min(int(floor((y_b - y_min)  / ((y_max - y_min) / n))), n-1);

      // compare the z-values
      int comp = comp_z_order(x_a_int, y_a_int, x_b_int, y_b_int);
      return comp <= 0;     
    });
    
    std::list<DataBlockEntry>::iterator it;
    // int count = 0;
    // std::cout << "data block entries size: " << data_block_entries_.size() << std::endl;
    for (it = data_block_entries_.begin(); it != data_block_entries_.end(); ++it) {
      if (it == --data_block_entries_.end()) {
        // std::cout << "last entry added" << std::endl;
        // count++;
        AddIdxEntry(*it, true);
      } else {
        AddIdxEntry(*it);
        // count++;
      }
      // AddIdxEntry(*it);
    }
    // AddIdxEntry(last_index_entry_, true);
    // std::cout << "count number: " << count << std::endl;
    // std::cout << "entries_ size: " << entries_.size() << std::endl;
  }
  
  if (partition_cnt_ == 0) {
    partition_cnt_ = entries_.size();
  }
  // It must be set to null after last key is added
  // assert(sub_index_builder_ == nullptr);

  if (finishing_indexes == true) {
    Entry& last_entry = entries_.front();
    if (firstlayer == true) {
      sec_entries_.emplace_back(std::make_pair(last_entry.key, last_partition_block_handle));      
    }

    if (sub_index_builder_ != nullptr) {
      std::string handle_encoding;
      last_partition_block_handle.EncodeTo(&handle_encoding);
      bool do_flush =
          partition_cut_requested_ ||
          flush_policy_->Update(last_entry.key, handle_encoding);
      if (do_flush) {
        // std::cout << "enclosing_mbr: " << enclosing_mbr_ << std::endl;
        next_level_entries_.push_back(
            {serializeMbrExcludeIID(enclosing_mbr_),
             std::unique_ptr<RtreeSecondaryIndexLevelBuilder>(sub_index_builder_)});
        enclosing_mbr_.clear();
        sub_index_builder_ = nullptr;
      }
    }
    if (sub_index_builder_ == nullptr) {
      MakeNewSubIndexBuilder();
    }
    sub_index_builder_->AddIndexEntry(last_partition_block_handle, last_entry.key);
    expandMbrExcludeIID(enclosing_mbr_, ReadValueMbr(last_entry.key));

    entries_.pop_front();
  }
  // If the current R-tree level has been constructed, move on to the next level.
  if (UNLIKELY(entries_.empty())) {

    // update R-tree height
    rtree_level_++;

    // PutVarint32(&rtree_height_str_, rtree_level_);
    // index_blocks->meta_blocks.insert(
    //   {kRtreeSecondaryIndexMetadataBlock.c_str(), rtree_height_str_});    

    // return Status::OK();

    firstlayer = false;
    if (sub_index_builder_ != nullptr){
      next_level_entries_.push_back(
          {serializeMbrExcludeIID(enclosing_mbr_),
          std::unique_ptr<RtreeSecondaryIndexLevelBuilder>(sub_index_builder_)});
      enclosing_mbr_.clear();
      sub_index_builder_ = nullptr;
    }

    // return if current level only contains one block
    // std::cout << "next_level_entries_ size: " << next_level_entries_.size() << std::endl;
    if (next_level_entries_.size() == 1) {
      Entry& entry = next_level_entries_.front();
      auto s = entry.value->Finish(index_blocks);
      // std::cout << "writing the top-level index block with enclosing MBR: " << ReadQueryMbr(entry.key) << std::endl;
      index_size_ += index_blocks->index_block_contents.size();
      PutVarint32(&rtree_height_str_, rtree_level_);
      index_blocks->meta_blocks.insert(
        {kRtreeSecondaryIndexMetadataBlock.c_str(), rtree_height_str_});
      // std::cout << "R-tree height: " << rtree_level_ << std::endl;
      return s;
    }

    // swaping the contents of entries_ and next_level_entries
    for (std::list<Entry>::iterator it = next_level_entries_.begin(), end = next_level_entries_.end(); it != end; ++it) {
      entries_.push_back({it->key, std::move(it->value)});
      // std::cout << "add new item to entries: " << ReadQueryMbr(it->key) << std::endl;
    }

    Entry& entry = entries_.front();
    auto s = entry.value->Finish(index_blocks);
    // std::cout << "writing an index block to disk with enclosing MBR: " << ReadSecQueryMbr(entry.key) << std::endl;
    index_size_ += index_blocks->index_block_contents.size();
    finishing_indexes = true;

    next_level_entries_.clear();
    // std::cout << "swapped next_level_entries and entries_, entries size: " << entries_.size() << std::endl;

    return Status::Incomplete();

    // index_blocks->index_block_contents = index_block_builder_.Finish();

    // top_level_index_size_ = index_blocks->index_block_contents.size();
    // index_size_ += top_level_index_size_;
    // return Status::OK();
  } else {
    // Finish the next partition index in line and Incomplete() to indicate we
    // expect more calls to Finish
    Entry& entry = entries_.front();

    auto s = entry.value->Finish(index_blocks);
    // std::cout << "writing an index block to disk with enclosing MBR: " << ReadSecQueryMbr(entry.key) << std::endl;
    index_size_ += index_blocks->index_block_contents.size();
    finishing_indexes = true;
    return s.ok() ? Status::Incomplete() : s;
  }
}

size_t RtreeSecondaryIndexBuilder::NumPartitions() const { return partition_cnt_; }

void RtreeSecondaryIndexBuilder::get_Secondary_Entries(
  std::vector<std::pair<std::string, BlockHandle>>* sec_entries) {
  *sec_entries = sec_entries_;
  sec_entries_.clear();
}

OneDRtreeSecondaryIndexBuilder* OneDRtreeSecondaryIndexBuilder::CreateIndexBuilder(
    const InternalKeyComparator* comparator,
    const bool use_value_delta_encoding,
    const BlockBasedTableOptions& table_opt) {
  return new OneDRtreeSecondaryIndexBuilder(comparator, table_opt,
                                     use_value_delta_encoding);
}

OneDRtreeSecondaryIndexBuilder::OneDRtreeSecondaryIndexBuilder(
    const InternalKeyComparator* comparator,
    const BlockBasedTableOptions& table_opt,
    const bool use_value_delta_encoding)
    : SecondaryIndexBuilder(comparator),
      index_block_builder_(table_opt.index_block_restart_interval,
                           true /*use_delta_encoding*/,
                           use_value_delta_encoding),
      sub_index_builder_(nullptr),
      table_opt_(table_opt),
      use_value_delta_encoding_(use_value_delta_encoding),
      rtree_level_(1) {}

OneDRtreeSecondaryIndexBuilder::~OneDRtreeSecondaryIndexBuilder() {
  delete sub_index_builder_;
}

void OneDRtreeSecondaryIndexBuilder::MakeNewSubIndexBuilder() {
  assert(sub_index_builder_ == nullptr);
  sub_index_builder_ = new OneDRtreeSecondaryIndexLevelBuilder(
      comparator_, table_opt_.index_block_restart_interval,
      table_opt_.format_version, use_value_delta_encoding_,
      table_opt_.index_shortening, /* include_first_key */ false);

  flush_policy_.reset(FlushBlockBySizePolicyFactory::NewFlushBlockPolicy(
      table_opt_.metadata_block_size, table_opt_.block_size_deviation,
      sub_index_builder_->index_block_builder_));
  partition_cut_requested_ = false;
}

void OneDRtreeSecondaryIndexBuilder::RequestPartitionCut() {
  partition_cut_requested_ = true;
}

void OneDRtreeSecondaryIndexBuilder::OnKeyAdded(const Slice& value){
    Slice val_temp = Slice(value);
    double numerical_val = *reinterpret_cast<const double*>(val_temp.data());
    ValueRange temp_val_range;
    temp_val_range.set_range(numerical_val, numerical_val);
    // expandValrange(sub_index_enclosing_valrange_, temp_val_range);
    tuple_valranges_.emplace_back(temp_val_range);
  }

void OneDRtreeSecondaryIndexBuilder::AddIndexEntry(
    std::string* last_key_in_current_block,
    const Slice* first_key_in_next_block, const BlockHandle& block_handle) {
  (void) first_key_in_next_block;
  std::string datablcoklastkeystr = std::string(*last_key_in_current_block);
  for (const ValueRange& v: tuple_valranges_) {
    DataBlockEntry dbe;
    dbe.datablockhandle = block_handle;
    dbe.datablocklastkey = datablcoklastkeystr;
    dbe.subindexenclosingvalrange = serializeValueRange(v);
    data_block_entries_.push_back(dbe);    
  }
  tuple_valranges_.clear();    
}

void OneDRtreeSecondaryIndexBuilder::AddIdxEntry(DataBlockEntry datablkentry, bool last) {
  expandValrange(enclosing_valrange_, ReadValueRange(datablkentry.subindexenclosingvalrange));
  // std::cout << "enclosing_mbr_: " << enclosing_mbr_ << std::endl;
  // Note: to avoid two consecuitive flush in the same method call, we do not
  // check flush policy when adding the last key
  if (UNLIKELY(last == true)) {  // no more keys
    if (sub_index_builder_ == nullptr) {
      MakeNewSubIndexBuilder();
    }
    sub_index_builder_->AddIndexEntry(datablkentry.datablockhandle, datablkentry.subindexenclosingvalrange);

    sub_index_last_key_ = serializeValueRange(enclosing_valrange_);
    entries_.push_back(
        {serializeValueRange(enclosing_valrange_),
         std::unique_ptr<OneDRtreeSecondaryIndexLevelBuilder>(sub_index_builder_)});
    enclosing_valrange_.clear();
    sub_index_builder_ = nullptr;
    cut_filter_block = true;
  } else {
    // apply flush policy only to non-empty sub_index_builder_
    if (sub_index_builder_ != nullptr) {
      std::string handle_encoding;
      std::string enclosing_valrange_encoding;
      enclosing_valrange_encoding = serializeValueRange(enclosing_valrange_);
      datablkentry.datablockhandle.EncodeTo(&handle_encoding);
      bool do_flush =
          partition_cut_requested_ ||
          flush_policy_->Update(enclosing_valrange_encoding, handle_encoding);
      if (do_flush) {
        // std::cout << "push_back a full sub_index builder" << std::endl;
        // std::cout << "pushed valuerange: " << enclosing_valrange_ << std::endl;
        entries_.push_back(
            {serializeValueRange(enclosing_valrange_),
             std::unique_ptr<OneDRtreeSecondaryIndexLevelBuilder>(sub_index_builder_)});
        enclosing_valrange_.clear();
        sub_index_builder_ = nullptr;
      }
    }
    if (sub_index_builder_ == nullptr) {
      MakeNewSubIndexBuilder();
    }
    sub_index_builder_->AddIndexEntry(datablkentry.datablockhandle, datablkentry.subindexenclosingvalrange);
    sub_index_last_key_ = serializeValueRange(enclosing_valrange_);

  }
}

Status OneDRtreeSecondaryIndexBuilder::Finish(
    IndexBlocks* index_blocks, const BlockHandle& last_partition_block_handle) {

  if (finishing_indexes == false) {
    
    data_block_entries_.sort([](const DataBlockEntry& a, const DataBlockEntry& b){
      ValueRange a_valrange = ReadValueRange(a.subindexenclosingvalrange);
      ValueRange b_valrange = ReadValueRange(b.subindexenclosingvalrange);

      // using the centre point of each interval for comparison
      double c_a = (a_valrange.range.min + a_valrange.range.max) / 2;
      double c_b = (b_valrange.range.min + b_valrange.range.max) / 2;

      return c_a <= c_b;     
    });
    
    std::list<DataBlockEntry>::iterator it;
    // int count = 0;
    // std::cout << "data block entries size: " << data_block_entries_.size() << std::endl;
    for (it = data_block_entries_.begin(); it != data_block_entries_.end(); ++it) {
      if (it == --data_block_entries_.end()) {
        // std::cout << "last entry added" << std::endl;
        // count++;
        AddIdxEntry(*it, true);
      } else {
        AddIdxEntry(*it);
        // count++;
      }
      // AddIdxEntry(*it);
    }
    // AddIdxEntry(last_index_entry_, true);
    // std::cout << "count number: " << count << std::endl;
    // std::cout << "entries_ size: " << entries_.size() << std::endl;
  }
  
  if (partition_cnt_ == 0) {
    partition_cnt_ = entries_.size();
  }

  if (finishing_indexes == true) {
    Entry& last_entry = entries_.front();

    if (firstlayer == true) {
      sec_entries_.emplace_back(std::make_pair(last_entry.key, last_partition_block_handle));
    }

    if (sub_index_builder_ != nullptr) {
      std::string handle_encoding;
      last_partition_block_handle.EncodeTo(&handle_encoding);
      bool do_flush =
          partition_cut_requested_ ||
          flush_policy_->Update(last_entry.key, handle_encoding);
      if (do_flush) {
        // std::cout << "enclosing_mbr: " << enclosing_mbr_ << std::endl;
        next_level_entries_.push_back(
            {serializeValueRange(enclosing_valrange_),
             std::unique_ptr<OneDRtreeSecondaryIndexLevelBuilder>(sub_index_builder_)});
        enclosing_valrange_.clear();
        sub_index_builder_ = nullptr;
      }
    }
    if (sub_index_builder_ == nullptr) {
      MakeNewSubIndexBuilder();
    }
    sub_index_builder_->AddIndexEntry(last_partition_block_handle, last_entry.key);
    expandValrange(enclosing_valrange_, ReadValueRange(last_entry.key));

    entries_.pop_front();
  }
  // If the current R-tree level has been constructed, move on to the next level.
  if (UNLIKELY(entries_.empty())) {

    // update R-tree height
    rtree_level_++;

    firstlayer = false;
    if (sub_index_builder_ != nullptr){
      next_level_entries_.push_back(
          {serializeValueRange(enclosing_valrange_),
          std::unique_ptr<OneDRtreeSecondaryIndexLevelBuilder>(sub_index_builder_)});
      enclosing_valrange_.clear();
      sub_index_builder_ = nullptr;
    }

    // return if current level only contains one block
    // std::cout << "next_level_entries_ size: " << next_level_entries_.size() << std::endl;
    if (next_level_entries_.size() == 1) {
      Entry& entry = next_level_entries_.front();
      auto s = entry.value->Finish(index_blocks);
      // std::cout << "writing the top-level index block with enclosing valuerange: " << ReadValueRange(entry.key) << std::endl;
      index_size_ += index_blocks->index_block_contents.size();
      PutVarint32(&rtree_height_str_, rtree_level_);
      index_blocks->meta_blocks.insert(
        {kRtreeSecondaryIndexMetadataBlock.c_str(), rtree_height_str_});
      // std::cout << "R-tree height: " << rtree_level_ << std::endl;
      return s;
    }

    // swaping the contents of entries_ and next_level_entries
    for (std::list<Entry>::iterator it = next_level_entries_.begin(), end = next_level_entries_.end(); it != end; ++it) {
      entries_.push_back({it->key, std::move(it->value)});
      // std::cout << "add new item to entries: " << ReadQueryMbr(it->key) << std::endl;
    }

    Entry& entry = entries_.front();
    auto s = entry.value->Finish(index_blocks);
    // std::cout << "writing an index block to disk with enclosing MBR: " << ReadSecQueryMbr(entry.key) << std::endl;
    index_size_ += index_blocks->index_block_contents.size();
    finishing_indexes = true;

    next_level_entries_.clear();
    // std::cout << "swapped next_level_entries and entries_, entries size: " << entries_.size() << std::endl;

    return Status::Incomplete();

    // index_blocks->index_block_contents = index_block_builder_.Finish();

    // top_level_index_size_ = index_blocks->index_block_contents.size();
    // index_size_ += top_level_index_size_;
    // return Status::OK();
  } else {
    // Finish the next partition index in line and Incomplete() to indicate we
    // expect more calls to Finish
    Entry& entry = entries_.front();

    auto s = entry.value->Finish(index_blocks);
    // std::cout << "writing an index block to disk with enclosing MBR: " << ReadSecQueryMbr(entry.key) << std::endl;
    index_size_ += index_blocks->index_block_contents.size();
    finishing_indexes = true;
    return s.ok() ? Status::Incomplete() : s;
  }
}

size_t OneDRtreeSecondaryIndexBuilder::NumPartitions() const { return partition_cnt_; }

void OneDRtreeSecondaryIndexBuilder::get_Secondary_Entries(
  std::vector<std::pair<std::string, BlockHandle>>* sec_entries) {
    *sec_entries = sec_entries_;
    sec_entries_.clear();
  }

}  // namespace ROCKSDB_NAMESPACE
