// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cstdio>
#include <string>
#include <iostream>
#include <sstream>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/flush_block_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/table.h"
#include "util/coding.h"
#include "util/rtree.h"
#include "util/hilbert_curve.h"
#include "util/z_curve.h"


using namespace rocksdb;

std::string kDBPath = "/tmp/test_db1";

std::string serialize_key(uint64_t iid, double xValue, double yValue) {
    std::string key;
    // The R-tree stores boxes, hence duplicate the input values
    key.append(reinterpret_cast<const char*>(&iid), sizeof(uint64_t));
    key.append(reinterpret_cast<const char*>(&xValue), sizeof(double));
    key.append(reinterpret_cast<const char*>(&xValue), sizeof(double));
    key.append(reinterpret_cast<const char*>(&yValue), sizeof(double));
    key.append(reinterpret_cast<const char*>(&yValue), sizeof(double));
    return key;
}


std::string serialize_query(uint64_t iid_min,
                            uint64_t iid_max, double x_value_min,
                            double x_value_max, double y_value_min,
                            double y_value_max) {
    std::string key;
    key.append(reinterpret_cast<const char*>(&iid_min), sizeof(uint64_t));
    key.append(reinterpret_cast<const char*>(&iid_max), sizeof(uint64_t));
    key.append(reinterpret_cast<const char*>(&x_value_min), sizeof(double));
    key.append(reinterpret_cast<const char*>(&x_value_max), sizeof(double));
    key.append(reinterpret_cast<const char*>(&y_value_min), sizeof(double));
    key.append(reinterpret_cast<const char*>(&y_value_max), sizeof(double));
    return key;
}

uint64_t decode_value(std::string& value) {
    return *reinterpret_cast<const uint64_t*>(value.data());
}

struct Key {
    Mbr mbr;
};


Key deserialize_key(Slice key_slice) {
    Key key;
    key.mbr = ReadKeyMbr(key_slice);
    return key;
}

// A comparator that interprets keys from Noise. It's a length prefixed
// string first (the keypath) followed by the value and the Internal Id.
class NoiseComparator : public rocksdb::Comparator {
public:
    const char* Name() const {
        return "rocksdb.NoiseComparator";
    }

    int Compare(const rocksdb::Slice& const_a, const rocksdb::Slice& const_b) const {
        Slice slice_a = Slice(const_a);
        Slice slice_b = Slice(const_b);
        // Slice keypath_a;
        // Slice keypath_b;
        // GetLengthPrefixedSlice(&slice_a, &keypath_a);
        // GetLengthPrefixedSlice(&slice_b, &keypath_b);

        // int keypath_compare = keypath_a.compare(keypath_b);
        // if (keypath_compare != 0) {
        //     return keypath_compare;
        // }

        // keypaths are the same, compare the value. The previous
        // `GetLengthPrefixedSlice()` did advance the Slice already, hence a call
        // to `.data()` can directly be used.
        const uint64_t* value_a = reinterpret_cast<const uint64_t*>(slice_a.data());
        const uint64_t* value_b = reinterpret_cast<const uint64_t*>(slice_b.data());

        if (*value_a < *value_b) {
            return -1;
        } else if (*value_a > *value_b) {
            return 1;
        } else {
            return 0;
        }
        if (*value_a < *value_b) {
            return -1;
        } else if (*value_a > *value_b) {
            return 1;
        } else {
            return 0;
        }

        // Specifically for R-tree as r-tree does not implement ordering
        // return 1;
        // return 1;
    }

    void FindShortestSeparator(std::string* start,
                               const rocksdb::Slice& limit) const {
        return;
    }

    void FindShortSuccessor(std::string* key) const  {
        return;
    }
};


int main() {
    DB* db;
    Options options;

    HilbertComparator cmp;
    options.comparator = &cmp;

    options.info_log_level = DEBUG_LEVEL;

    BlockBasedTableOptions block_based_options;

   block_based_options.index_type = BlockBasedTableOptions::kRtreeSearch;
//    block_based_options.index_type = BlockBasedTableOptions::kTwoLevelIndexSearch;
//    block_based_options.flush_block_policy_factory.reset(
//            new NoiseFlushBlockPolicyFactory());
    options.table_factory.reset(NewBlockBasedTableFactory(block_based_options));
    // options.memtable_factory.reset(new rocksdb::RTreeFactory);
    options.memtable_factory.reset(new rocksdb::SkipListMbrFactory);
    // options.allow_concurrent_memtable_write = false;

    for (int x1 = 0; x1 < 16; x1++) {
        for (int y1 = 0; y1 < 16; y1++) {
            for (int x2 = 0; x2 < 16; x2++) {
                for (int y2 = 0; y2 < 16; y2++) {
                    uint32_t z1 = xy2z(4, x1, y1);
                    uint32_t z2 = xy2z(4, x2, y2);
                    if ((z1 > z2) && (comp_z_order(x1, y1, x2, y2) != 1)) {
                        // std::cout << x1 << y1
                        std::cout << "error!";
                    }
                    if ((z1 < z2) && (comp_z_order(x1, y1, x2, y2) != -1)) {
                        std::cout << "error!";
                    }
                    if ((z1 == z2) && (comp_z_order(x1, y1, x2, y2) != 0)) {
                        std::cout << "error!";
                    }
                }
            }
        }
    }



    Status s;
    s = DB::Open(options, kDBPath, &db);
    std::cout << "Open DB status: " << s.ToString() << std::endl;
    // Failed to open, probably it doesn't exist yet. Try to create it and
    // insert data
    if (!s.ok()) {
        options.create_if_missing = true;
        s = DB::Open(options, kDBPath, &db);
        std::cout << "Create if missing: " << s.ToString() << std::endl;
        assert(s.ok());

        std::string key1 = serialize_key(1, 18.6598102, 73.4636206);
        // std::cout << "key1: " << key1 << std::endl;

        // Put key-value
        s = db->Put(WriteOptions(), key1, "key1");
        assert(s.ok());

        std::string key2 = serialize_key(2, 10.3234002, 76.1851883);
        // std::cout << "key2: " << key2 << std::endl;
        s = db->Put(WriteOptions(), key2, "");
        std::cout << s.ToString() << std::endl;
        assert(s.ok());

        std::string key3 = serialize_key(3, 25.0465603, 82.9407812);
        // std::cout << "key3: " << key3 << std::endl;
        s = db->Put(WriteOptions(), key3, "");
        assert(s.ok());

        std::string key4 = serialize_key(4, 16.7832288, 77.3759338);
        // std::cout << "key4: " << key4 << std::endl;
        s = db->Put(WriteOptions(), key4, "");
        std::cout << s.ToString() << std::endl;
        assert(s.ok());

        std::string key5 = serialize_key(5, 22.4813575, 88.3802035);
        // std::cout << "key5: " << key5 << std::endl;
        s = db->Put(WriteOptions(), key5, "");
        assert(s.ok());

        // cmp.Compare(Slice(key1), Slice(key2));
    }

    // Query the R-tree

    std::string value;

    // Specify the desired bounding box on the iterator
    rocksdb::ReadOptions read_options;
    rocksdb::RtreeIteratorContext iterator_context;

    // This scope is needed so that the unique pointer of the iterator runs
    // out of scope and cleans up things correctly
    // {
    //     iterator_context.query_mbr =
    //             serialize_query(0,5, 0, 100, 0, 1000000);
    //     read_options.iterator_context = &iterator_context;
    //     std::unique_ptr <rocksdb::Iterator> it(db->NewIterator(read_options));

    //     // std::cout << it->Valid() <<std::endl;

    //     std::cout << "query 1" << std::endl;
    //     // Iterate over the results and print the value
        
    //     // it->SeekToFirst();

    //     // Key key = deserialize_key(it->key());
    //     // std::cout << "Results: " << key.mbr << std::endl;
    //     // it->Next();
    //     // std::cout << "Valid(): " << it->Valid() << std::endl;

    //     // Key key2 = deserialize_key(it->key());
    //     // std::cout << "Results: " << key2.mbr << std::endl;
    //     // it->Next();
    //     // std::cout << "Valid(): " << it->Valid() << std::endl;



    //     // for (it->SeekToFirst(); it->Valid(); it->Next()) {
    //     //     // std::cout << "enter loop" << std::endl;
    //     //     Key key = deserialize_key(it->key());
    //     //     std::cout << "Results: " << key.mbr << std::endl;
    //     // }

    // }
    std::string key1 = serialize_key(1, 18.6598102, 73.4636206);
    // s = db->Get(read_options, key1, &value);
    // std::cout<< s.ToString() << std::endl;
    s = db->SpatialRange(read_options, key1, &value);
    std::cout << s.ToString() << std::endl;
    if (s.ok()) {
        std::cout << "value: " << value << std::endl;
    }

    s = db->Get(read_options, key1, &value);
    std::cout << s.ToString() << std::endl;
    if (s.ok()) {
        std::cout << "value: " << value << std::endl;
    }



    // {
    //     iterator_context.query_mbr =
    //             serialize_query(0, 1000000, 12, 30);
    //     read_options.iterator_context = &iterator_context;
    //     std::unique_ptr <rocksdb::Iterator> it(db->NewIterator(read_options));

    //     std::cout << "query 2" << std::endl;
    //     // Iterate over the results and print the value
    //     for (it->SeekToFirst(); it->Valid(); it->Next()) {
    //         Key key = deserialize_key(it->key());
    //         std::cout <<  "Results: " << key.mbr << std::endl;
    //     }
    // }

    // {
    //     iterator_context.query_mbr =
    //             serialize_query(0, 1000000, 0, 100);
    //     read_options.iterator_context = &iterator_context;
    //     std::unique_ptr <rocksdb::Iterator> it(db->NewIterator(read_options));

    //     std::cout << "query 3" << std::endl;
    //     // Iterate over the results and print the value
    //     for (it->SeekToFirst(); it->Valid(); it->Next()) {
    //         std::cout << "outer iterator valid: " << it->Valid() << std::endl;
    //         Key key = deserialize_key(it->key());
    //         std::cout <<  "Results: " << key.mbr << std::endl;
    //     }
    // }

    delete db;

    return 0;
}