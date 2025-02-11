# NEXT (ROCKSDB + LSM-based Secondary Index) 

![Alt text](/FrontPage.png)

This is a prototype implementation of NEXT for RocksDB. NEXT is a new LSM-based secondary index framework that utilizes a two-level structure to improve non-key attribute query performance. 
More information about NEXT can be found in the paper "NEXT: A New Secondary Index Framework for LSM-based Data Storage" which is accepted by SIGMOD 2025. The Figure below shows an overview of NEXT which consists of per-segment components in SST files and a global index component resides in RAM.

![Alt text](/next_overview.png)

## Usage

To install RocksDB: Visit https://github.com/facebook/rocksdb/blob/main/INSTALL.md .

To compile static library
```
cd rockdb-7.7.3
make clean
make static_lib
```

To run static spatial database writing
```
cd examples
make secondary_index_write
./secondary_index_write <Directory of the DB> <DB Size> <DB Source File>
```

To run static spatial database reading
```
cd examples
make secondary_index_read
./secondary_index_read <Directory of the DB> <Query Size> <Query File>
```

To run static numerical database writing
```
cd examples
make secondary_index_write_num
./secondary_index_write_num <Directory of the DB> <DB Size> <DB Source File>
```

To run static numerical database reading
```
cd examples
make secondary_index_read_num
./secondary_index_read_num <Directory of the DB> <Query Size> <Query File>
```
