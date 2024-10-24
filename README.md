# NEXT (ROCKSDB + LSM-based Secondary Index) 

![Alt text](/FrontPage.png)

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

To run static spatial database reading
```
cd examples
make secondary_index_read_num
./secondary_index_read_num <Directory of the DB> <Query Size> <Query File>
```
