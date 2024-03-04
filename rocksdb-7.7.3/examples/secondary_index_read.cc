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

std::string serialize_id(int iid) {
    std::string key;
    key.append(reinterpret_cast<const char*>(&iid), sizeof(int));
    return key;
}

std::string serialize_value(double xValue, double yValue) {
    std::string val;
    // The R-tree stores boxes, hence duplicate the input values
    val.append(reinterpret_cast<const char*>(&xValue), sizeof(double));
    val.append(reinterpret_cast<const char*>(&xValue), sizeof(double));
    val.append(reinterpret_cast<const char*>(&yValue), sizeof(double));
    val.append(reinterpret_cast<const char*>(&yValue), sizeof(double));
    return val;
}

std::string serialize_query(double x_value_min, double x_value_max,
                            double y_value_min, double y_value_max) {
    std::string query;
    query.append(reinterpret_cast<const char*>(&x_value_min), sizeof(double));
    query.append(reinterpret_cast<const char*>(&x_value_max), sizeof(double));
    query.append(reinterpret_cast<const char*>(&y_value_min), sizeof(double));
    query.append(reinterpret_cast<const char*>(&y_value_max), sizeof(double));
    return query;
}

uint64_t decode_value(std::string& value) {
    return *reinterpret_cast<const uint64_t*>(value.data());
}

struct Key {
    Mbr mbr;
};

struct Val {
    Mbr mbr;
};


Key deserialize_key(Slice key_slice) {
    Key key;
    key.mbr = ReadKeyMbr(key_slice);
    return key;
}

Val deserialize_val(Slice val_slice) {
    Val val;
    val.mbr = ReadValueMbr(val_slice);
    return val;
}

int main(int argc, char* argv[]) {

    std::string kDBPath = argv[1];
    int querySize = int(atoi(argv[2]));
    std::ifstream queryFile(argv[3]);
    std::cout << "Query size: " << querySize << std::endl;

    DB* db;
    Options options;

    options.info_log_level = DEBUG_LEVEL;
    options.statistics = rocksdb::CreateDBStatistics();

    BlockBasedTableOptions block_based_options;

    // Set the block cache to 64 MB
    block_based_options.block_cache = rocksdb::NewLRUCache(64 * 1024 * 1024);
    // block_based_options.index_type = BlockBasedTableOptions::KRtreeSecSearch;
    options.table_factory.reset(NewBlockBasedTableFactory(block_based_options));
    options.memtable_factory.reset(new rocksdb::SkipListSecFactory);

    Status s;
    s = DB::Open(options, kDBPath, &db);
    std::cout << "Open DB status: " << s.ToString() << std::endl;

    uint32_t id;
    uint32_t op;
    double low[2], high[2];

    rocksdb::ReadOptions read_options;
    rocksdb::RtreeIteratorContext iterator_context;

    std::chrono::nanoseconds totalDuration{0};
    for (int i=0; i<querySize;i++) {
        queryFile >> op >> id >> low[0] >> low[1] >> high[0] >> high[1];

        auto start = std::chrono::high_resolution_clock::now();
        iterator_context.query_mbr = 
                serialize_query(low[0], high[0], low[1], high[1]);
        read_options.iterator_context = &iterator_context;
        read_options.is_secondary_index_scan = true;
        std::unique_ptr <rocksdb::Iterator> it(db->NewIterator(read_options));

        int counter = 0;
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            Val value = deserialize_val(it->value());
            counter ++;
        }
        auto end = std::chrono::high_resolution_clock::now(); 
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
        totalDuration = totalDuration + duration;
        std::cout << "Total number of results: " << counter << std::endl;        
    }
    std::cout << "Execution time: " << totalDuration.count() << " nanoseconds" << std::endl;

    db->Close();    

    // for (int i=0; i < dataSize; i++) {
    //     std::string q_key = serialize_id(i);
    //     s = db->Get(ReadOptions(), q_key, &q_value);
    //     // Slice val_slice = decode_value(q_value);
    //     Val val = deserialize_val(q_value);
    //     std::cout << "query results: " << val.mbr.first << "," << val.mbr.second << std::endl;
    // }


    delete db;

    return 0;
}