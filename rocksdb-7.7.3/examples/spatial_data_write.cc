#include <algorithm>
#include <cstdio>
#include <string>
#include <fstream>
#include <iostream>
#include <unistd.h>
#include <sstream>
#include <vector>
#include <chrono>
#include <math.h>

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
                            uint64_t iid_max, double value_min,
                            double value_max) {
    std::string key;
    key.append(reinterpret_cast<const char*>(&iid_min), sizeof(uint64_t));
    key.append(reinterpret_cast<const char*>(&iid_max), sizeof(uint64_t));
    key.append(reinterpret_cast<const char*>(&value_min), sizeof(double));
    key.append(reinterpret_cast<const char*>(&value_max), sizeof(double));
    key.append(reinterpret_cast<const char*>(&value_min), sizeof(double));
    key.append(reinterpret_cast<const char*>(&value_max), sizeof(double));
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

        // // Specifically for R-tree as r-tree does not implement ordering
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

int main(int argc, char* argv[]) {

    std::string kDBPath = argv[1];

    int dataSize = int(atoi(argv[2]));
    std::ifstream dataFile(argv[3]);
    std::cout << "data size: " << dataSize << std::endl;

    DB* db;
    Options options;

    ZComparator cmp;
    // ZComparator cmp;
    // NoiseComparator cmp;
    options.comparator = &cmp;

    options.info_log_level = DEBUG_LEVEL;
    options.statistics = rocksdb::CreateDBStatistics();
    options.compaction_pri = kMinMbr;
    std::cout << "compatction_pri = " << options.compaction_pri << std::endl;
    // options.stats_dump_period_sec = 5;
    options.compaction_output_selection = kByScoreFunction;
    // options.compaction_output_selection = kByMbrOverlappingArea;
    // options.compaction_output_selection = OneDKeyRangeOnly;

    // Setting k using the max_compaction_output_files_selected
    // k is the number of files in the output level selected for compaction
    options.max_compaction_output_files_selected = 3;
    std::cout << "compatction_output_selection = " << options.compaction_output_selection << std::endl;

    BlockBasedTableOptions block_based_options;

    // Set the block cache to 64 MB
    block_based_options.block_cache = rocksdb::NewLRUCache(64 * 1024 * 1024);
    block_based_options.index_type = BlockBasedTableOptions::kRtreeSearch;
//    block_based_options.flush_block_policy_factory.reset(
//            new NoiseFlushBlockPolicyFactory());
    options.table_factory.reset(NewBlockBasedTableFactory(block_based_options));
    options.memtable_factory.reset(new rocksdb::SkipListMbrFactory);
    // options.memtable_factory.reset(new rocksdb::RTreeFactory);
    // options.allow_concurrent_memtable_write = false;

    // Set the write buffer size to 64 MB
    options.write_buffer_size = 64 * 1024 * 1024;

    // options.level0_file_num_compaction_trigger = 10;
    // options.max_bytes_for_level_base = 512 * 1024 * 1024;
    options.check_flush_compaction_key_order = false;
    options.force_consistency_checks = false;

    Status s;
    s = DB::Open(options, kDBPath, &db);
    std::cout << "Open DB status: " << s.ToString() << std::endl;

    uint32_t id;
    uint32_t op;
    double low[2], high[2];

    // Failed to open, probably it doesn't exist yet. Try to create it and
    // insert data
    if (!s.ok()) {
        options.create_if_missing = true;
        s = DB::Open(options, kDBPath, &db);
        std::cout << "Create if missing: " << s.ToString() << std::endl;
        assert(s.ok());

        std::cout << "start writing data" << std::endl;
        // auto totalDuration = std::chrono::duration<long long, std::milli>(0);
        std::chrono::nanoseconds totalDuration{0};
        for (int i = 0; i < dataSize; i++){
            dataFile >> op >> id >> low[0] >> low[1] >> high[0] >> high[1];
            // if (i < 50) {
            //     std::cout << op << " " << id << " " << low[0] << " " << low[1] << " " << high[0] << " " << high[1] << std::endl;
            // }

            std::string key = serialize_key(id, low[0], low[1]);

            // std::cout << "In Key: " << id << low[0] << low[1] << std::endl;
            // Put key-value
            auto start = std::chrono::high_resolution_clock::now();
            s = db->Put(WriteOptions(), key, "");
            auto end = std::chrono::high_resolution_clock::now(); 
            auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
            totalDuration = totalDuration + duration;
            // std::cout << "In Status: " << s.ToString() << std::endl;
            // assert(s.ok());
        }
        std::cout << "Status: " << s.ToString() << std::endl;
        assert(s.ok());

        // std::cout << "Status: " << s.ToString() << std::endl;

        std::cout << "end writing data" << std::endl;
        std::cout << "Execution time: " << totalDuration.count() << " nanoseconds" << std::endl;

        sleep(300);
        std::string stats_value;
        db->GetProperty("rocksdb.stats", &stats_value);
        std::cout << stats_value << std::endl;

        db->Close();



        // std::cout << "RocksDB stats: " << options.statistics->ToString() << std::endl;

        // std::string key1 = serialize_key(0, 516, 22.214);
        // std::cout << "key1: " << key1 << std::endl;

        // // Put key-value
        // s = db->Put(WriteOptions(), key1, "");
        // assert(s.ok());

        // std::string key2 = serialize_key(1, 1124, 4.1432);
        // std::cout << "key2: " << key2 << std::endl;
        // s = db->Put(WriteOptions(), key2, "");
        // assert(s.ok());
    }


    delete db;

    s = DB::Open(options, kDBPath, &db);
    // std::string stats_value;
    // db->GetProperty("rocksdb.stats", &stats_value);
    s = db->Close();

    // std::cout << stats_value << std::endl;

    std::cout << "RocksDB stats: " << options.statistics->ToString() << std::endl;


    return 0;
}