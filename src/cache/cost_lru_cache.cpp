#include <sstream>

#include "rocksdbapis.hpp"
#include "cost_lru_cache.hpp"

CostLRUCache::CostLRUCache(const std::string& cacheName_, long long cacheSize_, rocksdb::DB* db_) {
    cacheName = cacheName_;
    cacheSize = cacheSize_;
    db = db_;
}

long long CostLRUCache::get(std::string& key) {
    std::string newKey, value;
    encodeKey(key, newKey);
    bool exists = rd_get(db, newKey, value);
    if (exists) {
        markHit();
        std::string splits[3];
        valueToInfo(value, valueDelimiter, splits);
        long long size = std::stoll(splits[2]);
        put(key, size);
        return size;
    } else {
        markMiss();
        return 0LL;
    }
}

void CostLRUCache::put(std::string& key, long long size) {
    std::string encodedKey;
    encodeKey(key, encodedKey);
    std::string prv, nxt;

    std::string value;
    if (head == nullKey) {
        head = key;
        tail = key;
        prv = nullKey;
        nxt = nullKey;
    } else {
        bool exists = rd_get(db, encodedKey, value);
        if (exists) {
            CostLRUCache::del(key);
        }

        if (head == nullKey) {
            head = key;
            tail = key;
            prv = nullKey;
            nxt = nullKey;
        } else {
            std::string encodedHead;
            encodeKey(head, encodedHead);
            assert(rd_get(db, encodedHead, value));
            std::string splits[3];
            valueToInfo(value, valueDelimiter, splits);
            infoToValue(value, encodedKey, splits[1], splits[2]);
            rd_put(db, encodedHead, value);
            prv = nullKey;
            nxt = encodedHead;
            head = key;
        }
    }
    infoToValue(value, prv, nxt, std::to_string(size));
    rd_put(db, encodedKey, value);
    occupiedSize += size;

    while (occupiedSize > cacheSize) {
        CostLRUCache::del(tail);
    }
}

void CostLRUCache::del(std::string& key) {
    std::string encodedKey;
    encodeKey(key, encodedKey);
    std::string value;
    bool exists = rd_get(db, encodedKey, value);
    if (exists) {
        std::string splits[3];
        valueToInfo(value, valueDelimiter, splits);
        
        std::string prv = splits[0];
        std::string nxt = splits[1];
        long long size = std::stoll(splits[2]);
        occupiedSize -= size;
        rd_delete(db, encodedKey);

        if (prv == nullKey && nxt == nullKey) {
            head = nullKey;
            tail = nullKey;
        } else if (prv == nullKey) {
            std::string nxtValue;
            rd_get(db, nxt, nxtValue);
            std::string nxtSplits[3];
            valueToInfo(nxtValue, valueDelimiter, nxtSplits);
            infoToValue(nxtValue, nullKey, nxtSplits[1], nxtSplits[2]);
            rd_put(db, nxt, nxtValue);
            decodeKey(nxt, head);
        } else if (nxt == nullKey) {
            std::string prvValue;
            rd_get(db, prv, prvValue);
            std::string prvSplits[3];
            valueToInfo(prvValue, valueDelimiter, prvSplits);
            infoToValue(prvValue, prvSplits[0], nullKey, prvSplits[2]);
            rd_put(db, prv, prvValue);
            decodeKey(prv, tail);
        } else {
            std::string prvValue, nxtValue;
            rd_get(db, prv, prvValue);
            rd_get(db, nxt, nxtValue);
            std::string prvSplits[3], nxtSplits[3];
            valueToInfo(prvValue, valueDelimiter, prvSplits);
            valueToInfo(nxtValue, valueDelimiter, nxtSplits);
            infoToValue(prvValue, prvSplits[0], nxt, prvSplits[2]);
            infoToValue(nxtValue, prv, nxtSplits[1], nxtSplits[2]);
            rd_put(db, prv, prvValue);
            rd_put(db, nxt, nxtValue);   
        }
    }
}

void CostLRUCache::addBytesMiss(long long size) {
    tmpBytesMiss += size;
}

void CostLRUCache::getTmpHitMiss(int& hit_, int& miss_) {
    hit_ = tmpHit;
    miss_ = tmpMiss;
}

void CostLRUCache::getHitMiss(int& hit_, int& miss_) {
    hit_ = hit;
    miss_ = miss;
}

void CostLRUCache::getTmpBytesMiss(long long& tmpBytesMiss_) {
    tmpBytesMiss_ = tmpBytesMiss;
}

void CostLRUCache::resetTmps() {
    tmpHit = 0;
    tmpMiss = 0;
    tmpBytesMiss = 0LL;
}

void CostLRUCache::encodeKey(std::string key, std::string& newKey) {
    newKey = key + "*" + cacheName;
}

void CostLRUCache::decodeKey(std::string encodedKey, std::string& key) {
    size_t pos = encodedKey.find('*');
    assert(pos != std::string::npos);
    key = encodedKey.substr(0, pos);
}

void CostLRUCache::infoToValue(std::string& value, std::string& prv, std::string& nxt, const std::string& size) {
    value = prv + valueDelimiter + nxt + valueDelimiter + size;
}

void CostLRUCache::valueToInfo(const std::string& value, char delimiter, std::string results[3]) {
    std::istringstream stream(value);
    std::string token;
    int index = 0;

    while (std::getline(stream, token, delimiter) && index < 3) {
        results[index++] = token;
    }
}

void CostLRUCache::markHit() {
    tmpHit++;
    hit++;
}

void CostLRUCache::markMiss() {
    tmpMiss++;
    miss++;
}

