#include <cstdio>
#include <string>
#include <fstream>
#include <iostream>
#include <sstream>
#include <vector>
#include <chrono>

#include "rocksdb/db.h"
#include "rocksdb/flush_block_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/table.h"
#include "util/coding.h"
#include "util/rtree.h"


using namespace rocksdb;

std::string serialize_key(uint64_t iid, double x_value, double y_value) {
    std::string key;
    // The R-tree stores boxes, hence duplicate the input values
    key.append(reinterpret_cast<const char*>(&iid), sizeof(uint64_t));
    key.append(reinterpret_cast<const char*>(&x_value), sizeof(double));
    key.append(reinterpret_cast<const char*>(&x_value), sizeof(double));
    key.append(reinterpret_cast<const char*>(&y_value), sizeof(double));
    key.append(reinterpret_cast<const char*>(&y_value), sizeof(double));
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
    std::string keypath;
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
    }

    void FindShortestSeparator(std::string* start,
                               const rocksdb::Slice& limit) const {
        return;
    }

    void FindShortSuccessor(std::string* key) const  {
        return;
    }
};

int main(int argc, char* argv[]) {
    std::string kDBPath = argv[1];
    int querySize = int(atoi(argv[2]));
    std::ifstream queryFile(argv[3]);
    std::cout << "query size: " << querySize << std::endl;

    DB* db;
    Options options;

    NoiseComparator cmp;
    options.comparator = &cmp;

    BlockBasedTableOptions block_based_options;

    // Set the block cache to 64 MB
    block_based_options.block_cache = rocksdb::NewLRUCache(64 * 1024 * 1024);

//    block_based_options.index_type = BlockBasedTableOptions::kRtreeSearch;
//    block_based_options.flush_block_policy_factory.reset(
//            new NoiseFlushBlockPolicyFactory());
    options.table_factory.reset(NewBlockBasedTableFactory(block_based_options));
    options.memtable_factory.reset(new rocksdb::SkipListMbrFactory);

    // Set the write buffer size to 64 MB
    options.write_buffer_size = 64 * 1024 * 1024;


    Status s;
    s = DB::Open(options, kDBPath, &db);

    uint32_t id;
    uint32_t op;
    double low[2], high[2];

    std::string value;

    // Specify the desired bounding box on the iterator
    rocksdb::ReadOptions read_options;
    rocksdb::RtreeIteratorContext iterator_context;

    // This scope is needed so that the unique pointer of the iterator runs
    // out of scope and cleans up things correctly
    std::chrono::nanoseconds totalDuration{0};
    for (int i = 0; i < querySize; i++) {
        queryFile >> op >> id >> low[0] >> low[1] >> high[0] >> high[1];
        // if (i == 0) {
        //     std::cout << op << id << low[0] << low[1] << high[0] << high[1];
        // }
        auto start = std::chrono::high_resolution_clock::now();
        iterator_context.query_mbr =
                serialize_query(0, 10000000, low[0], high[0], low[1], high[1]);
        read_options.iterator_context = &iterator_context;
        std::unique_ptr <rocksdb::Iterator> it(db->NewIterator(read_options));

        int counter = 0;
        // Iterate over the results and print the value
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            Key key = deserialize_key(it->key());
            counter++;
        }
        auto end = std::chrono::high_resolution_clock::now(); 
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
        totalDuration = totalDuration + duration;
        std::cout << "Total number of results: " << counter << std::endl;
    }
    std::cout << "Execution time: " << totalDuration.count() << " nanoseconds" << std::endl;

    delete db;

    return 0;
}