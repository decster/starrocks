// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/field.h"
#include "column/vectorized_fwd.h"
#include "common/status.h"

namespace starrocks {

class RowStoreEncoder {
public:
    static void encode_keys(const vectorized::Schema& schema, const vectorized::Chunk& chunk, size_t offset, size_t len,
                            std::vector<std::string>& keys);
    static void encode_values(const vectorized::Schema& schema, const vectorized::Chunk& chunk, size_t offset,
                              size_t len, std::vector<std::string>& values);
    static Status decode(const std::vector<std::string>& keys, const std::vector<std::string>& values,
                       const vectorized::Schema& schema, vectorized::Chunk* dest);
};

} // namespace starrocks