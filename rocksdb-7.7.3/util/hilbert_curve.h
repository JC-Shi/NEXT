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

namespace rocksdb {

    extern void d2xy ( int n, int d, int &x, int &y );
    extern int i4_power ( int i, int j );
    extern void rot ( int n, int &x, int &y, int rx, int ry );
    extern int xy2d ( int n, int x, int y );

    class HilbertComparator : public rocksdb::Comparator {
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
            int m = 11;
            int n = 2048;

            int x_a_int = std::min(int(floor((x_a - x_min)  / ((x_max - x_min) / n))), n-1);
            int y_a_int = std::min(int(floor((y_a - y_min)  / ((y_max - y_min) / n))), n-1);
            int x_b_int = std::min(int(floor((x_b - x_min)  / ((x_max - x_min) / n))), n-1);
            int y_b_int = std::min(int(floor((y_b - y_min)  / ((y_max - y_min) / n))), n-1);

            // std::cout << x_a_int << " " << x_b_int << " " << y_a_int << " " << y_b_int << std::endl;
            // if (x_a_int < 0 || x_a_int > 2047 || y_a_int < 0 || y_a_int > 2047 || x_b_int < 0 || x_b_int > 2047 || y_b_int < 0 || y_b_int > 2047) {
            //     std::cout << "error!" << std::endl;
            // }

            int hilbert_a = xy2d(m, x_a_int, y_a_int);
            int hilbert_b = xy2d(m, x_b_int, y_b_int);

            // std::cout << hilbert_a << " " << hilbert_b << std::endl;

            if (hilbert_a < hilbert_b) {
                return -1;
            } else if (hilbert_a > hilbert_b) {
                return 1;
            } else {
                if (*value_a < *value_b) {
                    return -1;
                } else if (*value_a > *value_b) {
                    return 1;
                } else {
                    return 0;
                }
            }

            // TODO: options.compaction_policy query/write
            // Specifically for R-tree as r-tree does not implement ordering
            // (void) hilbert_a;
            // (void) hilbert_b;
            // (void) value_a;
            // (void) value_b;
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

}  // namespace rocksdb