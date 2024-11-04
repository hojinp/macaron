#ifndef ROCKSDBAPIS_HPP
#define ROCKSDBAPIS_HPP

#include <string>
#include <rocksdb/db.h>

// Function to create or open a RocksDB database
rocksdb::DB* rd_create_db(bool contd, const std::string& db_path, const std::string& file);

// Function to insert a key-value pair into the database
void rd_put(rocksdb::DB* db, const std::string& key, const std::string value);

// Function to retrieve a value associated with a key from the database
bool rd_get(rocksdb::DB* db, const std::string& key, std::string& value);

// Function to delete a key-value pair from the database
void rd_delete(rocksdb::DB* db, const std::string& key);

#endif // ROCKSDBAPIS_HPP