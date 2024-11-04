#include <sstream>

#include "rocksdbapis.hpp"
#include "latency_lru_cache.hpp"

LatencyLRUCache::LatencyLRUCache(const std::string& cacheName_, long long cacheSize_, rocksdb::DB* db_) {
    cacheName = cacheName_;
    cacheSize = cacheSize_;
    db = db_;
}

long long LatencyLRUCache::get(std::string& key) {
    std::string newKey, value;
    encodeKey(key, newKey);
    bool exists = rd_get(db, newKey, value);
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

void LatencyLRUCache::put(std::string& key, long long size, long long _birth) {
    std::string encodedKey;
    encodeKey(key, encodedKey);
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
        bool exists = rd_get(db, encodedKey, value);
        if (exists) { // exists in the cache: first delete the object, and then put it as a new head
            birth = LatencyLRUCache::del(key);
        } else { // does not exist: put it as a new head
            birth = _birth;
        }

        if (head == nullKey) { // after delete, the cache is empty
            head = key;
            tail = key;
            prv = nullKey;
            nxt = nullKey;
        } else { // after delete, the cache is still not empty
            std::string encodedHead;
            encodeKey(head, encodedHead);
            assert(rd_get(db, encodedHead, value) > 0LL);
            std::string _prv, _nxt;
            long long _size, _birth;
            valueToInfo(value, _prv, _nxt, _size, _birth);
            infoToValue(value, encodedKey, _nxt, _size, _birth);
            rd_put(db, encodedHead, value);
            prv = nullKey;
            nxt = encodedHead;
            head = key;
        }
    }
    assert(birth != -1LL);
    infoToValue(value, prv, nxt, size, birth);
    rd_put(db, encodedKey, value);
    occupiedSize += size;

    while (occupiedSize > cacheSize) {
        LatencyLRUCache::del(tail);
    }
}

// Return the birth of the deleted object if exists. Else, return -1LL.
long long LatencyLRUCache::del(std::string& key) {
    std::string encodedKey;
    encodeKey(key, encodedKey);
    std::string value;
    bool exists = rd_get(db, encodedKey, value);
    if (exists) {
        std::string prv, nxt;
        long long size, birth;
        valueToInfo(value, prv, nxt, size, birth);
        occupiedSize -= size;
        rd_delete(db, encodedKey);

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
            decodeKey(nxt, head);
        } else if (nxt == nullKey) {
            std::string prvValue;
            rd_get(db, prv, prvValue);
            std::string _prv, _nxt;
            long long _size, _birth;
            valueToInfo(prvValue, _prv, _nxt, _size, _birth);
            infoToValue(prvValue, _prv, nullKey, _size, _birth);
            rd_put(db, prv, prvValue);
            decodeKey(prv, tail);
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

long long LatencyLRUCache::getBirth(std::string& key) {
    std::string encodedKey;
    encodeKey(key, encodedKey);
    std::string value;
    bool exists = rd_get(db, encodedKey, value);
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

void LatencyLRUCache::updateCacheSize(long long cacheSize_) {
    if (cacheSize == cacheSize_)
        return;
    
    if (cacheSize < cacheSize_) {
        cacheSize = cacheSize_;
        return;
    }

    cacheSize = cacheSize_;
    while (occupiedSize > cacheSize) {
        LatencyLRUCache::del(tail);
    }
}

void LatencyLRUCache::encodeKey(std::string key, std::string& newKey) {
    newKey = key + "*" + cacheName;
}

void LatencyLRUCache::decodeKey(std::string encodedKey, std::string& key) {
    size_t pos = encodedKey.find('*');
    assert(pos != std::string::npos);
    key = encodedKey.substr(0, pos);
}

void LatencyLRUCache::infoToValue(std::string& value, std::string& prv, std::string& nxt, const long long& size, const long long& birth) {
    std::ostringstream oss;
    oss << prv << ',' << nxt << ',' << size << ',' << birth;
    value = oss.str();
}

void LatencyLRUCache::valueToInfo(const std::string& value, std::string& prv, std::string& nxt, long long& size, long long& birth) {
    std::istringstream iss(value);
    std::getline(iss, prv, ',');
    std::getline(iss, nxt, ',');
    iss >> size;
    iss.ignore(); // Ignore the comma
    iss >> birth;
}

