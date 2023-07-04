// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include "column/field.h"
#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "storage/row_store_encoder.h"
#include "types/bitmap_value.h"

namespace starrocks {

#define ROW_STORE_VERSION 0

// encode format as:
// Fixed column length 
// version | column num | null bitmap length| |null bitmap | colxx value (exclude null column) 

// Variable column length 
// offset encode for skip not readed columns, that is column projection
// version | column num | null bitmap length| null bitmap |string col1 end offset|string col1 value, int col2 value (exclude null column)

class RowStoreEncoderLength : public RowStoreEncoder {
public:
    Status encode_chunk_to_full_row_column(const Schema& schema, const Chunk& chunk,
                                           BinaryColumn* dest_column) override;
    Status decode_columns_from_full_row_column(const Schema& schema, const BinaryColumn& full_row_column,
                                                const std::vector<uint32_t>& read_column_ids,
                                                std::vector<std::unique_ptr<Column>>& dest) override;

private:
    void variable_slice_encode(std::vector<EncodeOp>* pops, int col_id, bool is_last = false) override;
    Status variable_slice_decode(Slice* src, std::string* dest, bool is_last = false) override;
};

} // namespace starrocks