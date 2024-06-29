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
// #include "util/z_curve.h"


using namespace rocksdb;

std::string serialize_id(int iid) {
    std::string key;
    key.append(reinterpret_cast<const char*>(&iid), sizeof(int));
    return key;
}

std::string serialize_value(double xValue) {
    std::string val;
    val.append(reinterpret_cast<const char*>(&xValue), sizeof(double));
    return val;
}

std::string serialize_query(double x_value_min, double x_value_max) {
    std::string query;
    query.append(reinterpret_cast<const char*>(&x_value_min), sizeof(double));
    query.append(reinterpret_cast<const char*>(&x_value_max), sizeof(double));
    return query;
}

double deserialize_val(Slice val_slice) {
    double val;
    val = *reinterpret_cast<const double*>(val_slice.data());
    return val;
}

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
        const int* value_a = reinterpret_cast<const int*>(slice_a.data());
        const int* value_b = reinterpret_cast<const int*>(slice_b.data());

        // if (*value_a < *value_b) {
        //     return -1;
        // } else if (*value_a > *value_b) {
        //     return 1;
        // } else {
        //     return 0;
        // }

        return slice_a.compare(slice_b);

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

    int operationSize = int(atoi(argv[2]));
    std::ifstream dataFile(argv[3]);
    std::cout << "data size: " << operationSize << std::endl;

    DB* db;
    Options options;

    NoiseComparator cmp;
    options.comparator = &cmp;

    options.info_log_level = DEBUG_LEVEL;
    options.statistics = rocksdb::CreateDBStatistics();

    options.max_write_buffer_number = 5;
    options.max_background_jobs = 8;   

    BlockBasedTableOptions block_based_options;
    
    // For per file secondary index in SST file
    block_based_options.create_secondary_index = true;
    block_based_options.create_sec_index_reader = true;
    block_based_options.sec_index_type = BlockBasedTableOptions::kOneDRtreeSec;
    
    // For global secondary index in memory
    options.create_global_sec_index = true;
    options.global_sec_index_loc = argv[4];
    options.global_sec_index_is_spatial = false;

    block_based_options.block_cache = rocksdb::NewLRUCache(64 * 1024 * 1024);

    // block_based_options.index_type = BlockBasedTableOptions::KRtreeSecSearch;
    options.table_factory.reset(NewBlockBasedTableFactory(block_based_options));
    options.memtable_factory.reset(new rocksdb::SkipListSecFactory);
    
    // options.memtable_factory.reset(new rocksdb::SkipListMbrFactory);
    // options.memtable_factory.reset(new rocksdb::RTreeFactory);
    options.allow_concurrent_memtable_write = false;
    options.force_consistency_checks = false;

    // Set the write buffer size to 64 MB
    options.write_buffer_size = 64 * 1024 * 1024;

    // options.level0_file_num_compaction_trigger = 2;
    // options.max_bytes_for_level_base = 512 * 1024 * 1024;
    // options.check_flush_compaction_key_order = false;
    // options.force_consistency_checks = false;

    Status s;
    s = DB::Open(options, kDBPath, &db);
    std::cout << "Open DB status: " << s.ToString() << std::endl;
    assert(s.ok());

    int id;
    double user_id;
    uint32_t op;
    double low[2], high[2];

    rocksdb::ReadOptions read_options;
    rocksdb::RtreeIteratorContext iterator_context;
    std::string operation_type;
    int lineCount = 0;
    std::string line;

    while(std::getline(dataFile, line)) {
        if (lineCount == operationSize) {
            break;
        }

        lineCount++;
        std::string token;
        std::istringstream ss(line);
        ss >> operation_type;

        if(operation_type == "w") {
            ss >> operation_type >> id >> low[0] >> low[1] >> high[0] >> high[1] >> user_id;

            std::string key = serialize_id(id);
            std::string value = serialize_value(user_id);

            while(std::getline(ss, token, '\t')) {
                value += token + "\t";
            }
            if(!value.empty() && value.back() == ' ') {
                value.pop_back();
            }

            auto start = std::chrono::high_resolution_clock::now();
            s = db->Put(WriteOptions(), key, value);
            assert(s.ok());
            auto end = std::chrono::high_resolution_clock::now();
            std::chrono::nanoseconds put_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
            std::cout << "Put Operation Latency: " << put_duration.count() << " nanoseconds" << std::endl;            
        }
        else if (operation_type == "rs") {
            ss >> operation_type >> low[0] >> low[1];

            iterator_context.query_mbr = 
                    serialize_query(low[0], low[1]);
            read_options.iterator_context = &iterator_context;
            read_options.is_secondary_index_scan = true;
            read_options.is_secondary_index_spatial = false;
            read_options.async_io = true;

            auto l_start = std::chrono::high_resolution_clock::now();
            std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(read_options));
            int counter = 0;
            for (it->SeekToFirst(); it->Valid(); it->Next()) {
                double value = deserialize_val(it->value());
                std::cout << "found value: " << value << std::endl;
                counter ++;
            }
            auto l_end = std::chrono::high_resolution_clock::now(); 
            std::chrono::nanoseconds l_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(l_end - l_start);
            std::cout << "LookUp Latency: " << l_duration.count() << " nanoseconds" << std::endl;                 
        }
        else if (operation_type == "up") {
            ss >> operation_type >> id >> low[0] >> low[1] >> high[0] >> high[1] >> user_id;

            std::string up_key = serialize_id(id);
            std::string up_value = serialize_value(user_id);

            while(std::getline(ss, token, '\t')) {
                up_value += token + "\t";
            }
            if(!up_value.empty() && up_value.back() == ' ') {
                up_value.pop_back();
            }

            auto up_start = std::chrono::high_resolution_clock::now();
            s = db->Delete(WriteOptions(), up_key);
            assert(s.ok());
            s = db->Put(WriteOptions(), up_key, up_value);
            assert(s.ok());
            auto up_end = std::chrono::high_resolution_clock::now();
            std::chrono::nanoseconds up_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(up_end - up_start);
            std::cout << "Update Operation Latency: " << up_duration.count() << " nanoseconds" << std::endl;               
        }
        else {
            std::cout << "unknown opeartion encountered" << std::endl;            
        }
    }

    sleep(300);
    db->Close();


    delete db;

    // s = DB::Open(options, kDBPath, &db);
    // // std::string stats_value;
    // // db->GetProperty("rocksdb.stats", &stats_value);
    // s = db->Close();

    // // std::cout << stats_value << std::endl;

    // std::cout << "RocksDB stats: " << options.statistics->ToString() << std::endl;


    return 0;
}