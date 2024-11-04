#include <sstream>
#include <fstream>
#include <iostream>
#include <vector>

#include "rocksdbapis.hpp"
#include "cost_lru_cache_lambda.hpp"

CostLRUCacheLambda::CostLRUCacheLambda(long long cacheSize_, rocksdb::DB* db_) {
    cacheSize = cacheSize_;
    db = db_;
}

void CostLRUCacheLambda::saveState(std::string& sizeFilePath) {
    std::ofstream sizeFile(sizeFilePath);
    if (!sizeFile.is_open()) {
        std::cerr << "Failed to open the file to save occupied size" << std::endl;
        return;
    }
    sizeFile << occupiedSize << "," << head << "," << tail << std::endl;
    sizeFile.close();
}

void CostLRUCacheLambda::loadState(std::string& sizeFilePath) {
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

long long CostLRUCacheLambda::get(std::string key) {
    std::string value;
    bool exists = rd_get(db, key, value);
    if (exists) {
        markHit();
        std::vector<std::string> splits;
        valueToInfo(value, splits);
        assert(splits.size() == 3);
        long long size = std::stoll(splits[2]);
        CostLRUCacheLambda::put(key, size);
        return size;
    } else {
        markMiss();
        return 0LL;
    }
}

void CostLRUCacheLambda::put(std::string key, long long size) {
    std::string prv, nxt, value;

    if (head == nullKey) {
        head = key;
        tail = key;
        prv = nullKey;
        nxt = nullKey;
    } else {
        bool exists = rd_get(db, key, value);
        if (exists) {
            CostLRUCacheLambda::del(key);
        }

        if (head == nullKey) {
            head = key;
            tail = key;
            prv = nullKey;
            nxt = nullKey;
        } else {
            assert(rd_get(db, head, value));
            std::vector<std::string> splits;
            valueToInfo(value, splits);
            assert(splits.size() == 3);
            value = infoToValue(key, splits[1], splits[2]);
            rd_put(db, head, value);
            prv = nullKey;
            nxt = head;
            head = key;
        }
    }
    value = infoToValue(prv, nxt, std::to_string(size));
    rd_put(db, key, value);
    occupiedSize += size;

    while (occupiedSize > cacheSize) {
        CostLRUCacheLambda::del(tail);
    }
}

void CostLRUCacheLambda::del(std::string key) {
    std::string value;

    bool exists = rd_get(db, key, value);
    if (exists) {
        std::vector<std::string> splits;
        valueToInfo(value, splits);
        assert(splits.size() == 3);
        std::string prv = splits[0];
        std::string nxt = splits[1];
        if (splits[2].size() == 0 || splits[2].find_first_not_of("0123456789") != std::string::npos) {
            std::cout << "splits[2] is not a number: " << splits[2] << std::endl;
        }
        long long size = std::stoll(splits[2]);
        occupiedSize -= size;
        rd_delete(db, key);

        if (prv == nullKey && nxt == nullKey) {
            head = nullKey;
            tail = nullKey;
        } else if (prv == nullKey) {
            std::string nxtValue;
            rd_get(db, nxt, nxtValue);
            std::vector<std::string> nxtSplits;
            valueToInfo(nxtValue, nxtSplits);
            assert(nxtSplits.size() == 3);
            nxtValue = infoToValue(nullKey, nxtSplits[1], nxtSplits[2]);
            rd_put(db, nxt, nxtValue);
            head = nxt;
        } else if (nxt == nullKey) {
            std::string prvValue;
            rd_get(db, prv, prvValue);
            std::vector<std::string> prvSplits;
            valueToInfo(prvValue, prvSplits);
            assert(prvSplits.size() == 3);
            prvValue = infoToValue(prvSplits[0], nullKey, prvSplits[2]);
            rd_put(db, prv, prvValue);
            tail = prv;
        } else {
            std::string prvValue, nxtValue;
            rd_get(db, prv, prvValue);
            rd_get(db, nxt, nxtValue);
            std::vector<std::string> prvSplits, nxtSplits;
            valueToInfo(prvValue, prvSplits);
            valueToInfo(nxtValue, nxtSplits);
            assert(prvSplits.size() == 3 && nxtSplits.size() == 3);
            prvValue = infoToValue(prvSplits[0], nxt, prvSplits[2]);
            nxtValue = infoToValue(prv, nxtSplits[1], nxtSplits[2]);
            rd_put(db, prv, prvValue);
            rd_put(db, nxt, nxtValue);   
        }
    }
}

void CostLRUCacheLambda::addBytesMiss(long long size) {
    tmpBytesMiss += size;
}

void CostLRUCacheLambda::getTmpHitMiss(int& hit_, int& miss_) {
    hit_ = tmpHit;
    miss_ = tmpMiss;
}

void CostLRUCacheLambda::getHitMiss(int& hit_, int& miss_) {
    hit_ = hit;
    miss_ = miss;
}

void CostLRUCacheLambda::getTmpBytesMiss(long long& tmpBytesMiss_) {
    tmpBytesMiss_ = tmpBytesMiss;
}

void CostLRUCacheLambda::resetTmps() {
    tmpHit = 0;
    tmpMiss = 0;
    tmpBytesMiss = 0LL;
}

std::string CostLRUCacheLambda::infoToValue(std::string prv, std::string nxt, std::string size) {
    return prv + valueDelimiter + nxt + valueDelimiter + size;
}

void CostLRUCacheLambda::valueToInfo(std::string value, std::vector<std::string>& results) {
    std::istringstream stream(value);
    std::string token;
    while (std::getline(stream, token, valueDelimiter) && results.size() < 3) {
        results.push_back(token);
    }
}

void CostLRUCacheLambda::markHit() {
    tmpHit++;
    hit++;
}

void CostLRUCacheLambda::markMiss() {
    tmpMiss++;
    miss++;
}

