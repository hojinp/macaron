#ifndef CACHEITEM_HPP
#define CACHEITEM_HPP

#include <string>

#include "enums.hpp"

class CacheItem {
public:
    CacheItem(std::string key, int offset, long long size, ObjectState state) : key(key), offset(offset), size(size), state(state) {}

    std::string key;
    int offset;
    long long size;
    ObjectState state;
};

#endif // CACHEITEM_HPP