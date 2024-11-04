#include <sstream>
#include <fstream>
#include <iostream>

#include "rocksdbapis.hpp"
#include "latency_lru_cache_lambda.hpp"

LatencyLRUCacheLambda::LatencyLRUCacheLambda(long long cacheSize_, rocksdb::DB* db_) {
    cacheSize = cacheSize_;
    db = db_;
}

long long LatencyLRUCacheLambda::get(std::string& key) {
    std::string value;
    bool exists = rd_get(db, key, value);
    if (exists) {
        std::string prv, nxt;
        long long size, birth;
        valueToInfo(value, prv, nxt, size, birth);
        put(key, size, -1LL);
        return size;
    } else {
        return 0LL;
    }
}

void LatencyLRUCacheLambda::put(std::string& key, long long size, long long _birth) {
    std::string prv, nxt;
    long long birth = -1LL;

    std::string value;
    if (head == nullKey) { // cache is empty
        head = key;
        tail = key;
        prv = nullKey;
        nxt = nullKey;
        birth = _birth;
    } else { // cache is not empty
        bool exists = rd_get(db, key, value);
        if (exists) { // exists in the cache: first delete the object, and then put it as a new head
            birth = LatencyLRUCacheLambda::del(key);
        } else { // does not exist: put it as a new head
            birth = _birth;
        }

        if (head == nullKey) { // after delete, the cache is empty
            head = key;
            tail = key;
            prv = nullKey;
            nxt = nullKey;
        } else { // after delete, the cache is still not empty
            assert(rd_get(db, head, value) > 0LL);
            std::string _prv, _nxt;
            long long _size, _birth;
            valueToInfo(value, _prv, _nxt, _size, _birth);
            infoToValue(value, key, _nxt, _size, _birth);
            rd_put(db, head, value);
            prv = nullKey;
            nxt = head;
            head = key;
        }
    }
    assert(birth != -1LL);
    infoToValue(value, prv, nxt, size, birth);
    rd_put(db, key, value);
    occupiedSize += size;

    while (occupiedSize > cacheSize) {
        LatencyLRUCacheLambda::del(tail);
    }
}

// Return the birth of the deleted object if exists. Else, return -1LL.
long long LatencyLRUCacheLambda::del(std::string& key) {
    std::string value;
    bool exists = rd_get(db, key, value);
    if (exists) {
        std::string prv, nxt;
        long long size, birth;
        valueToInfo(value, prv, nxt, size, birth);
        occupiedSize -= size;
        rd_delete(db, key);

        if (prv == nullKey && nxt == nullKey) {
            head = nullKey;
            tail = nullKey;
        } else if (prv == nullKey) {
            std::string nxtValue;
            rd_get(db, nxt, nxtValue);
            std::string _prv, _nxt;
            long long _size, _birth;
            valueToInfo(nxtValue, _prv, _nxt, _size, _birth);
            infoToValue(nxtValue, nullKey, _nxt, _size, _birth);
            rd_put(db, nxt, nxtValue);
            head = nxt;
        } else if (nxt == nullKey) {
            std::string prvValue;
            rd_get(db, prv, prvValue);
            std::string _prv, _nxt;
            long long _size, _birth;
            valueToInfo(prvValue, _prv, _nxt, _size, _birth);
            infoToValue(prvValue, _prv, nullKey, _size, _birth);
            rd_put(db, prv, prvValue);
            tail = prv;
        } else {
            std::string prvValue, nxtValue;
            rd_get(db, prv, prvValue);
            rd_get(db, nxt, nxtValue);
            std::string _prv, _nxt, __prv, __nxt;
            long long _size, _birth, __size, __birth;
            valueToInfo(prvValue, _prv, _nxt, _size, _birth);
            valueToInfo(nxtValue, __prv, __nxt, __size, __birth);
            infoToValue(prvValue, _prv, nxt, _size, _birth);
            infoToValue(nxtValue, prv, __nxt, __size, __birth);
            rd_put(db, prv, prvValue);
            rd_put(db, nxt, nxtValue);
        }
        return birth;
    }
    return -1LL;
}

long long LatencyLRUCacheLambda::getBirth(std::string& key) {
    std::string value;
    bool exists = rd_get(db, key, value);
    if (exists) {
        std::string prv, nxt;
        long long size, birth;
        valueToInfo(value, prv, nxt, size, birth);
        assert(birth != -1LL);
        return birth;
    } else {
        return -1LL;
    }
}

void LatencyLRUCacheLambda::updateCacheSize(long long cacheSize_) {
    if (cacheSize == cacheSize_)
        return;
    
    if (cacheSize < cacheSize_) {
        cacheSize = cacheSize_;
        return;
    }

    cacheSize = cacheSize_;
    while (occupiedSize > cacheSize) {
        LatencyLRUCacheLambda::del(tail);
    }
}


void LatencyLRUCacheLambda::saveState(std::string& sizeFilePath) {
    std::ofstream sizeFile(sizeFilePath);
    if (!sizeFile.is_open()) {
        std::cerr << "Failed to open the file to save occupied size" << std::endl;
        return;
    }
    sizeFile << occupiedSize << "," << head << "," << tail << std::endl;
    sizeFile.close();
}

void LatencyLRUCacheLambda::loadState(std::string& sizeFilePath) {
    std::ifstream sizeFile(sizeFilePath);
    if (!sizeFile.is_open()) {
        occupiedSize = 0LL;
        head = nullKey;
        tail = nullKey;
        return;
    }
    std::string line;
    std::getline(sizeFile, line);
    std::vector<std::string> splits;
    std::istringstream stream(line);
    std::string token;
    while (std::getline(stream, token, ',')) {
        splits.push_back(token);
    }
    if (splits.size() != 3) {
        std::cerr << "Invalid format of the file to load occupied size" << std::endl;
        return;
    }
    occupiedSize = std::stoll(splits[0]);
    head = splits[1];
    tail = splits[2];
    sizeFile.close();
}

void LatencyLRUCacheLambda::infoToValue(std::string& value, std::string& prv, std::string& nxt, const long long& size, const long long& birth) {
    std::ostringstream oss;
    oss << prv << ',' << nxt << ',' << size << ',' << birth;
    value = oss.str();
}

void LatencyLRUCacheLambda::valueToInfo(const std::string& value, std::string& prv, std::string& nxt, long long& size, long long& birth) {
    std::istringstream iss(value);
    std::getline(iss, prv, ',');
    std::getline(iss, nxt, ',');
    iss >> size;
    iss.ignore(); // Ignore the comma
    iss >> birth;
}

