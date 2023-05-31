//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Utility functions needed for the R-tree

#pragma once
#include <ostream>
#include <sstream>

#include "rocksdb/options.h"

namespace rocksdb {

// There's an interval (which might be collapsed to a point) for every
// dimension
    struct Interval {
        double min;
        double max;

        friend std::ostream& operator<<(std::ostream& os, const Interval& interval) {
            return os  << "[" << interval.min << "," << interval.max << "]";
        };

        friend std::ostream& operator<<(std::ostream& os, const std::vector<Interval>& intervals) {
            os << "[";
            bool first = true;
            for (auto& interval: intervals) {
                if (first) {
                    first = false;
                } else {
                    os << ",";
                }
                os  << interval;
            }
            return os << "]";
        };
    };

    struct RtreeIteratorContext: public IteratorContext {
        std::string query_mbr;
        RtreeIteratorContext(): query_mbr() {};
    };

    struct IntInterval {
        uint64_t min;
        uint64_t max;

        friend std::ostream& operator<<(std::ostream& os,
                                        const IntInterval& interval) {
            return os << "[" << interval.min << "," << interval.max << "]";
        };
        
        std::string toString() const {
            std::stringstream ss;
            ss << (*this);
            return ss.str();
        }
    };

    class Mbr {
    public:
        Mbr() : isempty_(true) {}

        // Whether any valued were set or not (true no values were set yet)
        bool empty() { return isempty_; };
        // Unset the Mbr
        void clear() { isempty_ = true; };

        void set_iid(const uint64_t min, const uint64_t max) {
            iid = {min, max};
            isempty_ = false;
        }
        void set_first(const double min, const double max) {
            first = {min, max};
            isempty_ = false;
        }
        void set_second(const double min, const double max) {
            second = {min, max};
            isempty_ = false;
        }

        // It's 3 dimensions with 64-bit min and max values
        size_t size() const {
            return 48;
        }

        friend std::ostream& operator<<(std::ostream& os, const Mbr& mbr) {
            return os << "[" << mbr.iid << "," << mbr.first << "," << mbr.second << "]";
        };

        std::string toString() const {
            std::stringstream ss;
            ss << (*this);
            return ss.str();
        }

        IntInterval iid;
        Interval first;
        Interval second;

    private:
        bool isempty_;
    };

    struct Rect {
        Rect () {}

        Rect(int a_minX, int a_minY, int a_maxX, int a_maxY)
        {
            min[0] = a_minX;
            min[1] = a_minY;

            max[0] = a_maxX;
            max[1] = a_maxY;
        }

        int min[2];
        int max[2];
    };

    extern double GetMbrArea(Mbr aa);
    extern double GetOverlappingArea(Mbr aa, Mbr bb);

    extern bool IntersectMbr(Mbr aa, Mbr bb);
    extern bool IntersectMbrExcludeIID(Mbr aa, Mbr bb);
    extern Mbr ReadKeyMbr(Slice data);

    // Reads the mbr (intervals) from the key. It modifies the key slice.
    extern Mbr ReadQueryMbr(Slice data);
    extern std::string serializeMbr(const Mbr& mbr);
    extern void expandMbr(Mbr& to_expand, Mbr expander);

}  // namespace rocksdb