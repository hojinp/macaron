#ifndef REDISAPI_HPP
#define REDISAPI_HPP

#include <string>
#include <unordered_set>

#include <sw/redis++/redis++.h>

class RedisClient {
public:
    RedisClient() {};

    void connect(std::string host, int port);

    void put(const std::string& key, const std::string& value);

    bool get(const std::string& key, std::string& value);

    void del(const std::string& key);

    void flushall();

    void scanAll(std::unordered_set<std::string>& keys);

    long long getMaxMemory();

private:
    std::string _redis_address;
    sw::redis::Redis* _redis;
    std::string host;
    int port;
};

#endif // REDISAPI_HPP