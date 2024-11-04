#include "redisapis.hpp"
#include <sw/redis++/connection.h>

void RedisClient::connect(std::string host, int port) {
    sw::redis::ConnectionOptions options;
    options.host = host;
    options.port = port;
    this->host = host;
    this->port = port;

    sw::redis::ConnectionPoolOptions poolOptions;
    poolOptions.size = std::thread::hardware_concurrency();

    _redis = new sw::redis::Redis(options, poolOptions);
    while (true) {
        try {
            _redis->ping();
            break;
        } catch (const sw::redis::Error& e) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }
}

void RedisClient::put(const std::string& key, const std::string& value) {
    _redis->set(key, value);
}

bool RedisClient::get(const std::string& key, std::string& value) {
    auto val = _redis->get(key);
    if (val) {
        value = *val;
        return true;
    }
    return false;
}

void RedisClient::del(const std::string& key) {
    _redis->del(key);
}

void RedisClient::flushall() {
    _redis->flushall();
}

void RedisClient::scanAll(std::unordered_set<std::string>& keys) {
    sw::redis::ConnectionOptions options;
    options.host = host;
    options.port = port;

    sw::redis::Redis* scanner = new sw::redis::Redis(options);

    auto cursor = 0LL;
    auto pattern = "*";
    auto count = 10;
    while (true) {
        cursor = scanner->scan(cursor, pattern, count, std::inserter(keys, keys.begin()));
        if (cursor == 0LL) {
            break;
        }
    }
    delete scanner;
}

