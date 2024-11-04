#include <iostream>
#include <filesystem>
#include <string>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include "rocksdbapis.hpp"

rocksdb::DB* rd_create_db(bool contd, const std::string& db_path, const std::string& file) {
    if (!contd) { // delete the existing directory path, if it exists
        try {
            std::filesystem::remove_all(db_path);
            std::filesystem::create_directories(db_path);
        } catch (const std::filesystem::filesystem_error& e) {
            std::cerr << "Failed to delete the existing DB: " << e.what() << std::endl;
        }
    }

    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::DB* db;
    rocksdb::Status status = rocksdb::DB::Open(options, db_path + "/" + file, &db);

    if (!status.ok()) {
        db->Close();
        throw std::runtime_error("Failed to open/create the DB: " + status.ToString());
    }

    return db;
}

void rd_put(rocksdb::DB* db, const std::string& key, std::string value) {
    rocksdb::Status status = db->Put(rocksdb::WriteOptions(), key, value);
    if (!status.ok()) {
        std::cerr << "Failed to put data in the DB: " << status.ToString() << std::endl;
    }
}

bool rd_get(rocksdb::DB* db, const std::string& key, std::string& value) {
    rocksdb::Status status = db->Get(rocksdb::ReadOptions(), key, &value);
    if (!status.ok()) {
        assert(status.IsNotFound());
        return false;
    }
    return true;
}

void rd_delete(rocksdb::DB* db, const std::string& key) {
    rocksdb::Status status = db->Delete(rocksdb::WriteOptions(), key);
    if (!status.ok()) {
        std::cerr << "Failed to delete data from the DB: " << status.ToString() << std::endl;
    }
}
