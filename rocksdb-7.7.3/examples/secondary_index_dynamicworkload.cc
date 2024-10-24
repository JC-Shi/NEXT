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

using namespace rocksdb;

std::string serialize_id(int iid) {
    std::string key;
    key.append(reinterpret_cast<const char*>(&iid), sizeof(int));
    return key;
}

std::string serialize_value(double xValueMin, double xValueMax, double yValueMin, double yValueMax, uint64_t tstamp) {
    std::string val;
    // The R-tree stores boxes, hence duplicate the input values
    val.append(reinterpret_cast<const char*>(&xValueMin), sizeof(double));
    val.append(reinterpret_cast<const char*>(&xValueMax), sizeof(double));
    val.append(reinterpret_cast<const char*>(&yValueMin), sizeof(double));
    val.append(reinterpret_cast<const char*>(&yValueMax), sizeof(double));
    val.append(reinterpret_cast<const char*>(&tstamp), sizeof(uint64_t));
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

uint64_t deserialize_val(Slice val_slice) {
    uint64_t tstamp;
    tstamp = *reinterpret_cast<const uint64_t*>(val_slice.data() + 32);
    return tstamp;
}

int deserialize_key(Slice val_slice) {
    int id;
    id = *reinterpret_cast<const int*>(val_slice.data());
    return id;
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
    std::cout << "operation size: " << dataSize << std::endl;

    DB* db;
    Options options;

    NoiseComparator cmp;
    options.comparator = &cmp;
    NoiseComparator1 sec_cmp;
    options.sec_comparator = &sec_cmp;

    options.info_log_level = DEBUG_LEVEL;
    options.statistics = rocksdb::CreateDBStatistics();

    options.max_write_buffer_number = 4;
    options.max_background_jobs = 8;

    BlockBasedTableOptions block_based_options;

    // For per file secondary index in SST file
    block_based_options.create_secondary_index = true;
    block_based_options.create_sec_index_reader = true;
    block_based_options.block_cache = rocksdb::NewLRUCache(64 * 1024 * 1024);
    
    // For global secondary index in memory
    options.create_global_sec_index = true;
    options.global_sec_index_loc = argv[4];
    std::string resultpath = argv[5];
    std::ofstream resFile(resultpath);

    options.table_factory.reset(NewBlockBasedTableFactory(block_based_options));
    options.memtable_factory.reset(new rocksdb::SkipListSecFactory);
    
    options.allow_concurrent_memtable_write = false;
    options.force_consistency_checks = false;

    // Set the write buffer size to 64 MB
    options.write_buffer_size = 16 * 1024 * 1024;

    Status s;
    s = DB::Open(options, kDBPath, &db);
    std::cout << "Open DB status: " << s.ToString() << std::endl;

    int id;
    uint32_t op;
    double low[2], high[2];

    rocksdb::ReadOptions read_options;
    rocksdb::RtreeIteratorContext iterator_context;
    std::string operation_type;
    int lineCount = 0;
    std::string line;    

    std::chrono::nanoseconds totalDuration{0};
    std::map<int, uint64_t> update_memo;
    uint64_t time_stamp = 20000001;

    int write_c = 0;
    int query_c = 0;
    int update_c = 0;
    int unexpected = 0;

    while(std::getline(dataFile, line)) {
        if(lineCount == dataSize){
            break;
        }
        lineCount++;
        std::string token;
        std::istringstream ss(line);
        ss >> operation_type;
        // std::cout << lineCount << " " << operation_type << std::endl;
        int counter;

        if (operation_type == "w"){
            write_c ++;

            ss >> id >> low[0] >> low[1] >> high[0] >> high[1];
            auto write_start = std::chrono::high_resolution_clock::now();

            std::string key = serialize_id(id);
            std::string value = serialize_value(low[0], high[0], low[1], high[1], time_stamp);
            time_stamp ++;

            while(std::getline(ss, token, '\t')) {
                value += token + "\t";
            }
            if(!value.empty() && value.back() == ' ') {
                value.pop_back();
            }
            
            s = db->Put(WriteOptions(), key, value);
            // std::cout << "write first data" << std::endl;
            auto write_end = std::chrono::high_resolution_clock::now(); 
            auto write_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(write_end - write_start);
            totalDuration = totalDuration + write_duration;
        } else if (operation_type == "rs") {
            query_c ++;

            ss >> low[0] >> low[1] >> high[0] >> high[1];

            auto query_start = std::chrono::high_resolution_clock::now();
            iterator_context.query_mbr = 
                    serialize_query(low[0], high[0], low[1], high[1]);
            read_options.iterator_context = &iterator_context;
            read_options.is_secondary_index_scan = true;
            // read_options.async_io = true;
            // std::cout << "create newiterator" << std::endl;
            std::unique_ptr <rocksdb::Iterator> it(db->NewIterator(read_options));
            // std::cout << "created New iterator" << std::endl;
            counter = 0;
            // auto start = std::chrono::high_resolution_clock::now();
            for (it->SeekToFirst(); it->Valid(); it->Next()) {
                int pkey_id = deserialize_key(it->key());
                // Val value = deserialize_val(it->value());
                uint64_t timestamp = deserialize_val(it->value());
                if (update_memo.count(pkey_id) > 0 && update_memo[pkey_id] > timestamp) {
                    continue;
                } else {
                    counter ++;
                }
                // counter++;
            }

            // std::cout << "found results: " << counter << std::endl;
            auto query_end = std::chrono::high_resolution_clock::now(); 
            auto query_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(query_end - query_start);
            totalDuration = totalDuration + query_duration;

            if(counter < 1){
                unexpected ++;
            }

            it.reset();
        } else if (operation_type == "up") {
            update_c ++;

            ss >> id >> low[0] >> low[1] >> high[0] >> high[1];
            auto update_start = std::chrono::high_resolution_clock::now();

            std::string key = serialize_id(id);
            std::string value = serialize_value(low[0], high[0], low[1], high[1], time_stamp);

            while(std::getline(ss, token, '\t')) {
                value += token + "\t";
            }
            if(!value.empty() && value.back() == ' ') {
                value.pop_back();
            }
            
            s = db->Delete(WriteOptions(), key);
            s = db->Put(WriteOptions(), key, value);
            update_memo[id] = time_stamp;
            time_stamp++;
            // std::cout << "write first data" << std::endl;
            auto update_end = std::chrono::high_resolution_clock::now(); 
            auto update_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(update_end - update_start);
            totalDuration = totalDuration + update_duration;

        }

        if (lineCount % 50000 == 0) {
            std::cout << totalDuration.count() << "\t" << counter << std::endl;
        }
        if (lineCount % 100000 == 0) {
            std::cout << "Total operations @" << lineCount << ": \t" << totalDuration.count() << "\t nanoseconds" << std::endl;
            resFile << "Opeartions : \t" << lineCount << "\t" << totalDuration.count() << "\n";
            
            // auto s_ro = ro_db->TryCatchUpWithPrimary();
            // std::cout << "Catchup DB status: " << s_ro.ToString() << std::endl;

        }


    }

    std::cout << "Execution time: " << totalDuration.count() << "nanoseconds" << std::endl;
    resFile << "Total Duration: \t" << totalDuration.count() << "\n";
    std::cout << "Total number of write: " << write_c << "; Total number of queries: " << query_c << "; Total number of updates: " << update_c << std::endl; 


    db->Close();
    resFile.close();    

    delete db;

    return 0;
}