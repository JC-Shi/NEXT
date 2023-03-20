//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <vector>

#include "util/rtree.h"

namespace rocksdb {

    bool IntersectMbr(Mbr aa,
                      Mbr bb) {
        // If a bounding region is empty, return true
        // if aa or bb is empty, then may indicate full table scan
        if (aa.empty() || bb.empty()) {
            return true;
        }

        // If the bounding regions don't intersect in one dimension, they won't
        // intersect at all, hence we can return early
        if (aa.iid.min > bb.iid.max || bb.iid.min > aa.iid.max) {
            return false;
        }
        if (aa.first.min > bb.first.max || bb.first.min > aa.first.max) {
            return false;
        }
        if (aa.second.min > bb.second.max || bb.second.min > aa.second.max) {
            return false;
        }
        return true;
    }

    void ReadMbrValues(Mbr& mbr, Slice& data) {
        double min = *reinterpret_cast<const double*>(data.data());
        double max = *reinterpret_cast<const double*>(data.data() + 8);
        mbr.set_first(min, max);
        min = *reinterpret_cast<const double*>(data.data() + 16);
        max = *reinterpret_cast<const double*>(data.data() + 24);
        mbr.set_second(min, max);
    }

    Mbr ReadKeyMbr(Slice data) {
        Mbr mbr;
        // In a key the first dimension is a single value only
        const uint64_t iid = *reinterpret_cast<const uint64_t*>(data.data());
        mbr.set_iid(iid, iid);
        data.remove_prefix(sizeof(uint64_t));
        ReadMbrValues(mbr, data);
        return mbr;
    }

    Mbr ReadQueryMbr(Slice data) {
        Mbr mbr;
        // In a key the first dimension is a single value only
        const uint64_t iid_min = *reinterpret_cast<const uint64_t*>(data.data());
        const uint64_t iid_max =
                *reinterpret_cast<const uint64_t*>(data.data() + sizeof(uint64_t));
        mbr.set_iid(iid_min, iid_max);
        data.remove_prefix(2 * sizeof(uint64_t));
        ReadMbrValues(mbr, data);
        return mbr;
    }

    std::string serializeMbr(const Mbr& mbr) {
        std::string serialized;
        serialized.append(reinterpret_cast<const char*>(&mbr.iid.min),
                        sizeof(uint64_t));
        serialized.append(reinterpret_cast<const char*>(&mbr.iid.max),
                        sizeof(uint64_t));
        serialized.append(reinterpret_cast<const char*>(&mbr.first.min),
                        sizeof(double));
        serialized.append(reinterpret_cast<const char*>(&mbr.first.max),
                        sizeof(double));
        serialized.append(reinterpret_cast<const char*>(&mbr.second.min),
                        sizeof(double));
        serialized.append(reinterpret_cast<const char*>(&mbr.second.max),
                        sizeof(double));
        return serialized;
  }

}  // namespace rocksdb