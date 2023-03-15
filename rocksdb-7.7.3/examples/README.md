1. Compile RocksDB first by executing `make static_lib` in parent dir
2. Compile all examples: `cd examples/; make all`

Compile spatial query examples: `make spatial_data_write; make spatial_range_query`

To load a dataset:
```
./spatial_data_write [db_path] [data_size] [dataset_path]
```

To run spatial query:
```
./spatial_range_query [db_path] [query_size] [query_path]
```
