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

std::string serialize_key(uint64_t iid, double xValueMin, double xValueMax, double yValueMin, double yValueMax) {
    std::string key;
    // The R-tree stores boxes, hence duplicate the input values
    key.append(reinterpret_cast<const char*>(&iid), sizeof(uint64_t));
    key.append(reinterpret_cast<const char*>(&xValueMin), sizeof(double));
    key.append(reinterpret_cast<const char*>(&xValueMax), sizeof(double));
    key.append(reinterpret_cast<const char*>(&yValueMin), sizeof(double));
    key.append(reinterpret_cast<const char*>(&yValueMax), sizeof(double));
    return key;
}

std::string serialize_id(int iid) {
    std::string key;
    key.append(reinterpret_cast<const char*>(&iid), sizeof(int));
    return key;
}

std::string serialize_value(double xValueMin, double xValueMax, double yValueMin, double yValueMax) {
    std::string val;
    // The R-tree stores boxes, hence duplicate the input values
    val.append(reinterpret_cast<const char*>(&xValueMin), sizeof(double));
    val.append(reinterpret_cast<const char*>(&xValueMax), sizeof(double));
    val.append(reinterpret_cast<const char*>(&yValueMin), sizeof(double));
    val.append(reinterpret_cast<const char*>(&yValueMax), sizeof(double));
    return val;
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
        const int* value_a = reinterpret_cast<const int*>(slice_a.data());
        const int* value_b = reinterpret_cast<const int*>(slice_b.data());

        // if (*value_a < *value_b) {
        //     return -1;
        // } else if (*value_a > *value_b) {
        //     return 1;
        // } else {
        //     return 0;
        // }
        // Similar to default comparator setting
        return slice_a.compare(slice_b);

    }

    void FindShortestSeparator(std::string* start,
                               const rocksdb::Slice& limit) const {
        return;
    }

    void FindShortSuccessor(std::string* key) const  {
        return;
    }
};

class NoiseComparator1 : public rocksdb::Comparator {
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

        // specific comparator to allow random output order
        return 1;
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

    NoiseComparator cmp;
    options.comparator = &cmp;
    NoiseComparator1 sec_cmp;
    options.sec_comparator = &sec_cmp;

    options.info_log_level = DEBUG_LEVEL;
    options.statistics = rocksdb::CreateDBStatistics();

    options.max_write_buffer_number = 5;
    options.max_background_jobs = 8;

    BlockBasedTableOptions block_based_options;
    
    // For per file secondary index in SST file
    block_based_options.create_secondary_index = true;
    block_based_options.create_sec_index_reader = true;
    
    // For global secondary index in memory
    options.create_global_sec_index = true;

    options.table_factory.reset(NewBlockBasedTableFactory(block_based_options));
    options.memtable_factory.reset(new rocksdb::SkipListSecFactory);
    
    options.allow_concurrent_memtable_write = false;

    // Set the write buffer size to 64 MB
    options.write_buffer_size = 64 * 1024 * 1024;

    Status s;

    int id;
    uint32_t op;
    double perimeter;
    double low[2], high[2];

    // Failed to open, probably it doesn't exist yet. Try to create it and
    // insert data
    if (true) {
        options.create_if_missing = true;
        s = DB::Open(options, kDBPath, &db);
        std::cout << "Create if missing: " << s.ToString() << std::endl;
        assert(s.ok());

        std::cout << "start writing data" << std::endl;
        // auto totalDuration = std::chrono::duration<long long, std::milli>(0);
        std::chrono::nanoseconds totalDuration{0};

        std::string line;
        int lineCount = 0;
        while(std::getline(dataFile, line)) {
            if(lineCount == dataSize){
                break;
            }
            lineCount++;
            std::string token;
            std::istringstream ss(line);

            // ss >> id >> low[0] >> low[1] >> high[0] >> high[1];
            ss >> id >> perimeter >> low[0] >> low[1] >> high[0] >> high[1];

            std::string key = serialize_id(id);
            std::string value = serialize_value(low[0], high[0], low[1], high[1]);

            while(std::getline(ss, token, '\t')) {
                value += token + "\t";
            }
            if(!value.empty() && value.back() == ' ') {
                value.pop_back();
            }

            auto start = std::chrono::high_resolution_clock::now();
            s = db->Put(WriteOptions(), key, value);
            // std::cout << "write first data" << std::endl;
            auto end = std::chrono::high_resolution_clock::now(); 
            auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
            totalDuration = totalDuration + duration;
        }
        
        std::cout << "Status: " << s.ToString() << std::endl;
        assert(s.ok());

        std::cout << "end writing data" << std::endl;
        std::cout << "Execution time: " << totalDuration.count() << " nanoseconds" << std::endl;

        // sleep to complete the background compactions 
        sleep(480);

        db->Close();

    }

    delete db;

    return 0;
}