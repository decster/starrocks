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

#include "storage/row_store_encoder.h"
#include "storage/primary_key_encoder.h"
#include "storage/row_store_encoder_util.h"

#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "gutil/endian.h"
#include "storage/chunk_helper.h"
#include "storage/olap_common.h"
#include "storage/tablet_schema.h"
#include "types/date_value.hpp"

namespace starrocks {

Status RowStoreEncoder::encode_chunk_to_full_row_column(const Schema& schema, const Chunk& chunk,
                                                        BinaryColumn* dest_column) {
    // chunk have both key and value
    int num_col = schema.num_fields() - schema.num_key_fields();
    std::vector<EncodeOp> ops(num_col);
    std::vector<const void*> datas(num_col);
    prepare_ops_datas_selective(schema, schema.num_key_fields(), schema.num_fields() - 1, &datas, chunk, &ops);
    size_t num_rows = chunk.num_rows();
    dest_column->reserve(dest_column->size() + num_rows);
    string buff;
    for (size_t i = 0; i < num_rows; i++) {
        buff.clear();
        for (int j = 0; j < num_col; j++) {
            ops[j](datas[j], i, &buff);
        }
        dest_column->append(buff);
    }
    return Status::OK();
}

Status RowStoreEncoder::encode_columns_to_full_row_column(const Schema& schema, const std::vector<Column*>& columns,
                                                          BinaryColumn& dest) {
    // columns only include values used, idx is offset, (value index - key end index)
    int num_col = columns.size();
    std::vector<EncodeOp> ops(num_col);
    std::vector<const void*> datas(num_col);
    prepare_ops_datas_selective_for_columns(schema, schema.num_key_fields(), columns.size(), &datas, columns, &ops);
    size_t num_row = columns[0]->size();
    dest.reserve(dest.size() + num_row);
    string buff;
    for (size_t i = 0; i < num_row; i++) {
        buff.clear();
        for (int j = 0; j < num_col; j++) {
            ops[j](datas[j], i, &buff);
        }
        dest.append(buff);
    }
    return Status::OK();
}

Status RowStoreEncoder::decode_columns_from_full_row_column(const Schema& schema, const BinaryColumn& full_row_column,
                                                             const std::vector<uint32_t>& read_column_ids,
                                                             std::vector<std::unique_ptr<Column>>& dest) {
    for (size_t i = 0; i < full_row_column.size(); i++) {
        Slice s = full_row_column.get_slice(i);
        uint32_t cur_read_idx = 0;
        for (uint j = schema.num_key_fields(); j <= read_column_ids.back(); j++) {
            Column* dest_column = nullptr;
            if (read_column_ids[cur_read_idx] == j) {
                dest_column = dest[cur_read_idx].get();
                cur_read_idx++;
            }
            switch (schema.field(j)->type()->type()) {
            case TYPE_BOOLEAN: {
                uint8_t v;
                decode_integral(&s, &v);
                if (dest_column) dest_column->append_datum(Datum(v));
            } break;
            case TYPE_TINYINT: {
                int8_t v;
                decode_integral(&s, &v);
                if (dest_column) dest_column->append_datum(Datum(v));
            } break;
            case TYPE_SMALLINT: {
                int16_t v;
                decode_integral(&s, &v);
                if (dest_column) dest_column->append_datum(Datum(v));
            } break;
            case TYPE_INT: {
                int32_t v;
                decode_integral(&s, &v);
                if (dest_column) dest_column->append_datum(Datum(v));
            } break;
            case TYPE_BIGINT: {
                int64_t v;
                decode_integral(&s, &v);
                if (dest_column) dest_column->append_datum(Datum(v));
            } break;
            case TYPE_LARGEINT: {
                int128_t v;
                decode_integral(&s, &v);
                if (dest_column) dest_column->append_datum(Datum(v));
            } break;
            case TYPE_VARCHAR: {
                string v;
                decode_slice(&s, &v, (j == schema.num_fields() - 2) || (j == schema.num_key_fields() - 1));
                if (dest_column) dest_column->append_datum(Datum(v));
            } break;
            case TYPE_DATE: {
                DateValue v;
                decode_integral(&s, &v._julian);
                if (dest_column) dest_column->append_datum(Datum(v));
            } break;
            case TYPE_DATETIME: {
                TimestampValue v;
                decode_integral(&s, &v._timestamp);
                if (dest_column) dest_column->append_datum(Datum(v));
            } break;
            default:
                CHECK(false) << "type not supported for primary key encoding";
            }
        }
    }
    return Status::OK();
}

void RowStoreEncoder::prepare_ops_datas_selective(const Schema& schema, int from_col, int to_col,
                                                  std::vector<const void*>* pdatas, const Chunk& chunk,
                                                  std::vector<EncodeOp>* pops) {
    // chunk only include to encode data
    int num_all_cols = chunk.columns().size();
    // only encode value column
    std::vector<Column*> columns;
    for (int i = 0; i < num_all_cols; i++) {
        if (i >= from_col && i <= to_col){
            columns.emplace_back(chunk.columns()[i].get());
        }
    }
    prepare_ops_datas_selective_for_columns(schema, from_col, to_col, pdatas, columns, pops);
}

void RowStoreEncoder::prepare_ops_datas_selective_for_columns(const Schema& schema, int from_col, int to_col,
                                             std::vector<const void*>* pdatas, const std::vector<Column*>& columns,
                                             std::vector<EncodeOp>* pops) {
    // columns only include value columns                                                
    auto& ops = *pops;
    auto& datas = *pdatas;
    int col_num = to_col - from_col + 1;
    for (int j = 0; j < col_num; j++) {
        datas[j] = columns[j]->raw_data();
        switch (schema.field(from_col + j)->type()->type()) {
        case TYPE_BOOLEAN:
            ops[j] = [](const void* data, int idx, string* buff) {
                encode_integral(((const uint8_t*)data)[idx], buff);
            };
            break;
        case TYPE_TINYINT:
            ops[j] = [](const void* data, int idx, string* buff) { encode_integral(((const int8_t*)data)[idx], buff); };
            break;
        case TYPE_SMALLINT:
            ops[j] = [](const void* data, int idx, string* buff) {
                encode_integral(((const int16_t*)data)[idx], buff);
            };
            break;
        case TYPE_INT:
            ops[j] = [](const void* data, int idx, string* buff) {
                encode_integral(((const int32_t*)data)[idx], buff);
            };
            break;
        case TYPE_BIGINT:
            ops[j] = [](const void* data, int idx, string* buff) {
                encode_integral(((const int64_t*)data)[idx], buff);
            };
            break;
        case TYPE_LARGEINT:
            ops[j] = [](const void* data, int idx, string* buff) {
                encode_integral(((const int128_t*)data)[idx], buff);
            };
            break;
        case TYPE_VARCHAR:
            variable_slice_encode(pops, j, j == col_num - 1);
            break;
        case TYPE_DATE:
            ops[j] = [](const void* data, int idx, string* buff) {
                encode_integral(((const int32_t*)data)[idx], buff);
            };
            break;
        case TYPE_DATETIME:
            ops[j] = [](const void* data, int idx, string* buff) {
                encode_integral(((const int64_t*)data)[idx], buff);
            };
            break;
        default:
            CHECK(false) << "type not supported for primary key encoding "
                         << logical_type_to_string(schema.field(j)->type()->type());
        }
    }
}

void RowStoreEncoder::combine_key_with_ver(std::string& key, const int8_t op, const int64_t version) {
    uint32_t key_len = key.length();
    encode_integral(encode_version(op, version), &key);
    encode_integral(key_len, &key);
}

Status RowStoreEncoder::split_key_with_ver(const std::string& ckey, std::string& key, int8_t& op, int64_t& version) {
    Slice len_slice = Slice(ckey.data() + ckey.length() - 4, 4);
    Slice ver_slice = Slice(ckey.data() + ckey.length() - 12, 8);
    uint32_t len = 0;
    int64_t ver = 0;
    decode_integral(&len_slice, &len);
    decode_integral(&ver_slice, &ver);
    CHECK(ckey.length() == len + 12) << "split_key_with_ver error";
    key = ckey.substr(0, len);
    decode_version(ver, op, version);
    return Status::OK();
}

void RowStoreEncoder::variable_slice_encode(std::vector<EncodeOp>* pops, int col_id, bool is_last) {
    auto& ops = *pops;
    is_last ? ops[col_id] = [](const void* data, int idx, string* buff) { encode_slice(((const Slice*)data)[idx], buff, true); }
            : ops[col_id] = [](const void* data, int idx, string* buff) { encode_slice(((const Slice*)data)[idx], buff, false);};
}

Status RowStoreEncoder::variable_slice_decode(Slice* src, std::string* dest, bool is_last) {
    return decode_slice(src, dest, is_last);
}

} // namespace starrocks