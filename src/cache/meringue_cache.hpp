#ifndef MERINGUECACHE_HPP
#define MERINGUECACHE_HPP

#include <string>
#include <vector>
#include <list>
#include <unordered_map>
#include <iostream>
#include <fstream>

class MeringueCache {
public:
    MeringueCache(long long cacheSize_);

    long long get(std::string key);
    void putWithoutEviction(std::string key, long long value);
    void del(std::string key);

    void setNewCacheSize(long long cacheSize_);
    void evict(std::vector<std::string>& evictedItems);

    long long getOccupiedSize();
    long long getCacheSize();

    void sortAndWrite(std::string filename, long long totalSizeMB);

private:
    std::list<std::pair<std::string, long long>> items;
    std::unordered_map<std::string, std::list<std::pair<std::string, long long>>::iterator> cacheItemsMap;

    long long cacheSize;
    long long occupiedSize = 0LL;
};

#endif // MERINGUECACHE_HPP