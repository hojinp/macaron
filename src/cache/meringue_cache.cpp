#include "meringue_cache.hpp"

MeringueCache::MeringueCache(long long cacheSize_) {
    cacheSize = cacheSize_;
}

long long MeringueCache::get(std::string key) {
    auto it = cacheItemsMap.find(key);
    if (it == cacheItemsMap.end()) {
        return -1L;
    }

    items.splice(items.begin(), items, it->second);
    return it->second->second;
}

void MeringueCache::putWithoutEviction(std::string key, long long value) {
    auto it = cacheItemsMap.find(key);
    if (it != cacheItemsMap.end()) {
        occupiedSize += value - it->second->second;
        it->second->second = value;
        items.splice(items.begin(), items, it->second);
        return;
    }

    occupiedSize += value;
    items.emplace_front(key, value);
    cacheItemsMap[key] = items.begin();
}

void MeringueCache::del(std::string key) {
    auto it = cacheItemsMap.find(key);
    if (it == cacheItemsMap.end()) {
        return;
    }

    occupiedSize -= it->second->second;
    items.erase(it->second);
    cacheItemsMap.erase(it);
}

void MeringueCache::evict(std::vector<std::string>& evictedItems) {
    while (occupiedSize > cacheSize) {
        std::string lastItemName = items.back().first;
        evictedItems.push_back(lastItemName);
        del(lastItemName);
    }
}

void MeringueCache::setNewCacheSize(long long cacheSize_) {
    cacheSize = cacheSize_;
}

long long MeringueCache::getOccupiedSize() {
    return occupiedSize;
}

long long MeringueCache::getCacheSize() {
    return cacheSize;
}

void MeringueCache::sortAndWrite(std::string filename, long long totalSizeMB) {
    long long totalSizeBytes = totalSizeMB * 1024 * 1024;
    std::ofstream file(filename, std::ios::binary);
    for (auto& item : items) {
        size_t keySize = item.first.size();
        file.write(reinterpret_cast<const char*>(&keySize), sizeof(keySize));
        file.write(item.first.c_str(), keySize);
        file.write(reinterpret_cast<const char*>(&item.second), sizeof(item.second));
        totalSizeBytes -= item.second;
        if (totalSizeBytes <= 0) {
            break;
        }
    }
    file.close();
}
