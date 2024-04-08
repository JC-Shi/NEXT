#include<iostream>
#include "util/z_curve.h"
#include "rocksdb/options.h"

namespace rocksdb {

    bool less_msb(uint32_t x, uint32_t y) {
        return (x < y) && (x < (x ^ y));
    }

    int comp_z_order (uint32_t x_a_int, uint32_t y_a_int, uint32_t x_b_int, uint32_t y_b_int) {
        if (less_msb(x_a_int ^ x_b_int, y_a_int ^ y_b_int)) {
            if (y_a_int < y_b_int) {
                return -1;
            }
            else if (y_a_int > y_b_int) {
                return 1;
            }
        }
        if (x_a_int < x_b_int) {
            return -1;
        }
        else if (x_a_int > x_b_int) {
            return 1;
        }
        return 0;
    }

    uint32_t xy2z(int level, uint32_t x, uint32_t y) {
        uint32_t key = 0;
        for (int i = 0; i < level; i++) {
            key |= (x & 1) << (2 * i + 1);
            key |= (y & 1) << (2 * i);
            x >>= 1;
            y >>= 1;
        }
        return key;
    }    

}