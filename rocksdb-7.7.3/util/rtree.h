//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Utility functions needed for the R-tree

#pragma once
#include <ostream>
#include <sstream>
#include <algorithm>
#include <math.h>

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

    class SpatialSketch {
    public:
        SpatialSketch() {
            x_min_ = -12.2304942;
            x_max_ = 37.4497039;
            y_min_ = 50.0218541;
            y_max_ = 125.9548288;
            for(int i = 0; i < ROWS; i++) {
                for(int j = 0; j < COLS; j++) {
                    density_map_[i][j] = 0;
                }
            }
        }
        // SpatialSketch(double x_min, double x_max, double y_min, double y_max) {
        //     x_min_ = x_min;
        //     x_max_ = x_max;
        //     y_min_ = y_min;
        //     y_max_ = y_max;
        //     for(int i = 0; i < ROWS; i++) {
        //         for(int j = 0; j < COLS; j++) {
        //             density_map_[i][j] = 0;
        //         }
        //     }
        // }

        void addMbr(Mbr mbr) {
            double x_center = (mbr.first.min + mbr.first.max) / 2;
            double y_center = (mbr.second.min + mbr.second.max) / 2;

            int x_int = std::min(int(floor((x_center - x_min_)  / ((x_max_ - x_min_) / ROWS))), ROWS-1);
            int y_int = std::min(int(floor((y_center - y_min_)  / ((y_max_ - y_min_) / COLS))), COLS-1);
            density_map_[x_int][y_int] += 1;
        }

        friend std::ostream& operator<<(std::ostream& os, const SpatialSketch& sketch) {
            for (int i = 0; i < ROWS; i++) {
                for (int j = 0; j < ROWS; j++) {
                    os << sketch.density_map_[i][j] << " ";
                }
                os << std::endl;
            }
            return os;
        };

        std::string toString() const {
            std::stringstream ss;
            ss << (*this);
            return ss.str();
        }

    private:
        static const int ROWS = 16;
        static const int COLS = 16;
        uint32_t density_map_[ROWS][COLS];
        double x_min_;
        double x_max_;
        double y_min_;
        double y_max_;
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