//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Utility functions needed for the R-tree

#pragma once

#include <algorithm>
#include <math.h>
#include "rocksdb/options.h"
#include "util/rtree.h"

namespace rocksdb {

    extern int comp_z_order (uint32_t x_a_int, uint32_t y_a_int, uint32_t x_b_int, uint32_t y_b_int);
    extern uint32_t xy2z(int level, uint32_t x, uint32_t y);

    class ZComparator : public rocksdb::Comparator {
    public:
        const char* Name() const {
            return "rocksdb.HilbertComparator";
        }

        int Compare(const rocksdb::Slice& const_a, const rocksdb::Slice& const_b) const {
            Slice slice_a = Slice(const_a);
            Slice slice_b = Slice(const_b);

            // keypaths are the same, compare the value. The previous
            // `GetLengthPrefixedSlice()` did advance the Slice already, hence a call
            // to `.data()` can directly be used.
            const uint64_t* value_a = reinterpret_cast<const uint64_t*>(slice_a.data());
            const uint64_t* value_b = reinterpret_cast<const uint64_t*>(slice_b.data());

            double x_a = *reinterpret_cast<const double*>(slice_a.data() + 8);
            double x_b = *reinterpret_cast<const double*>(slice_b.data() + 8);
            double y_a = *reinterpret_cast<const double*>(slice_a.data() + 24);
            double y_b = *reinterpret_cast<const double*>(slice_b.data() + 24);

            // std::cout << x_a << " " << x_b << " " << y_a << " " << y_b << std::endl;

            double x_min = -12.2304942;
            double x_max = 37.4497039;
            double y_min = 50.0218541;
            double y_max = 125.9548288;
            // int m = 11;
            int n = 262144;

            uint32_t x_a_int = std::min(int(floor((x_a - x_min)  / ((x_max - x_min) / n))), n-1);
            uint32_t y_a_int = std::min(int(floor((y_a - y_min)  / ((y_max - y_min) / n))), n-1);
            uint32_t x_b_int = std::min(int(floor((x_b - x_min)  / ((x_max - x_min) / n))), n-1);
            uint32_t y_b_int = std::min(int(floor((y_b - y_min)  / ((y_max - y_min) / n))), n-1);

            // std::cout << x_a_int << " " << x_b_int << " " << y_a_int << " " << y_b_int << std::endl;
            // if (x_a_int < 0 || x_a_int > 2047 || y_a_int < 0 || y_a_int > 2047 || x_b_int < 0 || x_b_int > 2047 || y_b_int < 0 || y_b_int > 2047) {
            //     std::cout << "error!" << std::endl;
            // }
            // uint32_t bit = 1;

            int comp = comp_z_order(x_a_int, y_a_int, x_b_int, y_b_int);
            if (comp != 0) {
                return comp;
            }

            if (*value_a < *value_b) {
                return -1;
            } else if (*value_a > *value_b) {
                return 1;
            } else {
                return 0;
            }


            // TODO: options.compaction_policy query/write
            // Specifically for R-tree as r-tree does not implement ordering
            // return 1;
        }

        void FindShortestSeparator(std::string* start,
                                const rocksdb::Slice& limit) const {
            (void)start;
            (void)limit;
            return;
        }

        void FindShortSuccessor(std::string* key) const  {
            (void)key;
            return;
        }
    };


    class ZComparator4SecondaryIndex : public rocksdb::Comparator {
    public:
        const char* Name() const {
            return "rocksdb.HilbertComparator4SecondaryIndex";
        }

        int Compare(const rocksdb::Slice& const_a, const rocksdb::Slice& const_b) const {
            Slice slice_a = Slice(const_a);
            Slice slice_b = Slice(const_b);

            Mbr a_mbr = ReadSecQueryMbr(slice_a);
            Mbr b_mbr = ReadSecQueryMbr(slice_b);

            double x_a = (a_mbr.first.min + a_mbr.first.max) / 2;
            double y_a = (a_mbr.second.min + a_mbr.second.max) / 2;
            double x_b = (b_mbr.first.min + b_mbr.first.max) / 2;
            double y_b = (b_mbr.second.min + b_mbr.second.max) / 2;

            double x_min = -12.2304942;
            double x_max = 37.4497039;
            double y_min = 50.0218541;
            double y_max = 125.9548288;
            // int m = 11;
            int n = 8192;

            uint32_t x_a_int = std::min(int(floor((x_a - x_min)  / ((x_max - x_min) / n))), n-1);
            uint32_t y_a_int = std::min(int(floor((y_a - y_min)  / ((y_max - y_min) / n))), n-1);
            uint32_t x_b_int = std::min(int(floor((x_b - x_min)  / ((x_max - x_min) / n))), n-1);
            uint32_t y_b_int = std::min(int(floor((y_b - y_min)  / ((y_max - y_min) / n))), n-1);

            // std::cout << x_a_int << " " << x_b_int << " " << y_a_int << " " << y_b_int << std::endl;
            // if (x_a_int < 0 || x_a_int > 2047 || y_a_int < 0 || y_a_int > 2047 || x_b_int < 0 || x_b_int > 2047 || y_b_int < 0 || y_b_int > 2047) {
            //     std::cout << "error!" << std::endl;
            // }
            // uint32_t bit = 1;

            // FOR WRITE
            // int comp = comp_z_order(x_a_int, y_a_int, x_b_int, y_b_int);
            // if (comp != 0) {
            //     return comp;
            // } else {
            //     return 0;
            // }

            // TODO: options.compaction_policy query/write
            // Specifically for R-tree as r-tree does not implement ordering
            // FOR READ
            (void) x_a_int;
            (void) x_b_int;
            (void) y_a_int;
            (void) y_b_int;
            return 1;
        }

        void FindShortestSeparator(std::string* start,
                                const rocksdb::Slice& limit) const {
            (void)start;
            (void)limit;
            return;
        }

        void FindShortSuccessor(std::string* key) const  {
            (void)key;
            return;
        }
    };    

}  // namespace rocksdb