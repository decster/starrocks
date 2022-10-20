// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/field.h"
#include "column/vectorized_fwd.h"
#include "common/status.h"

namespace starrocks {

// op
#define DEL_OP 0
#define PUT_OP 1
// only use 56bit version
#define VER_MASK 0xFFFFFFFFFFFFFF
// key format:
// | raw_key | OP 8bit | version 56bit(reserse) | raw_key length 32bit |

inline int64_t encode_version(const int8_t op, const int64_t version) {
    return (((int64_t)op) << 56) | (VER_MASK - (version & VER_MASK));
}

inline void decode_version(const int64_t raw_version, int8_t& op, int64_t& version) {
    version = VER_MASK - (raw_version & VER_MASK);
    op = (int8_t)((raw_version >> 56) & 0xF);
}

class RowStoreEncoder {
public:
    static void chunk_to_keys(const vectorized::Schema& schema, const vectorized::Chunk& chunk, size_t offset,
                              size_t len, std::vector<std::string>& keys);
    static void chunk_to_values(const vectorized::Schema& schema, const vectorized::Chunk& chunk, size_t offset,
                                size_t len, std::vector<std::string>& values);
    static Status kvs_to_chunk(const std::vector<std::string>& keys, const std::vector<std::string>& values,
                               const vectorized::Schema& schema, vectorized::Chunk* dest);
    //static void combine_key_with_ver(std::string& key, const int8_t op, );
};

} // namespace starrocks