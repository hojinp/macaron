#ifndef ENUMS_H
#define ENUMS_H

enum PackingMode {
    NO_PACKING = 1,
    WITH_PACKING = 2
};

enum CacheLevelMode {
    SINGLE_LEVEL = 1,
    TWO_LEVEL = 2
};

enum CloudServiceProvider {
    AWS = 1,
    AZURE = 2
};

enum CacheAlgorithm {
    LRU = 1
};

enum CloudOperation {
    PUT = 1,
    GET = 2,
    DELETE = 3
};

enum ObjectState {
    VALID = 1,
    EVICTED = 2,
    DELETED = 3
};

#endif // ENUMS_H
