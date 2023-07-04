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
#include "storage/row_store_encoder_factory.h"

#include <gtest/gtest.h>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/datum.h"
#include "column/schema.h"
#include "fs/fs_util.h"
#include "gutil/stringprintf.h"
#include "storage/chunk_helper.h"

using namespace std;

namespace starrocks {

static unique_ptr<Schema> create_schema(const vector<pair<LogicalType, bool>>& types) {
    Fields fields;
    for (int i = 0; i < types.size(); i++) {
        string name = StringPrintf("col%d", i);
        auto fd = new Field(i, name, types[i].first, false);
        fd->set_is_key(types[i].second);
        fd->set_aggregate_method(STORAGE_AGGREGATE_NONE);
        fields.emplace_back(fd);
    }
    return unique_ptr<Schema>(new Schema(std::move(fields), PRIMARY_KEYS, {}));
}

TEST(RowStoreEncoderTest, testEncodeVersion) {
    auto row_encoder = RowStoreEncoderFactory::instance()->get_or_create_encoder(SIMPLE);
    for (int i = 0; i < 1000; i++) {
        string tmpstr = StringPrintf("slice000%d", i * 17);
        int8_t op = i % 2;
        int64_t ver = i * 1000;
        string buf = tmpstr;
        row_encoder->combine_key_with_ver(buf, op, ver);
        int8_t dop = 0;
        int64_t dver = 0;
        string dstr;
        row_encoder->split_key_with_ver(buf, dstr, dop, dver);
        ASSERT_EQ(op, dop);
        ASSERT_EQ(ver, dver);
        ASSERT_EQ(tmpstr, dstr);
    }
}

TEST(RowStoreEncoderTest, testEncodeFullRowColumn) {
    // init schema
    auto schema = create_schema({{TYPE_INT, true}, {TYPE_VARCHAR, true}, {TYPE_INT, false}, {TYPE_BOOLEAN, false}});
    auto schema_with_row = create_schema({{TYPE_INT, true}, {TYPE_VARCHAR, true}, {TYPE_INT, false}, {TYPE_BOOLEAN, false}, {TYPE_VARCHAR, false}});
    const int n = 100;
    auto pchunk = ChunkHelper::new_chunk(*schema, n);
    for (int i = 0; i < n; i++) {
        Datum tmp;
        string tmpstr = StringPrintf("slice000%d", i * 17);
        if (i % 5 == 0) {
            // set some '\0'
            tmpstr[rand() % tmpstr.size()] = '\0';
        }
        tmp.set_int32(i * 2343);
        pchunk->columns()[0]->append_datum(tmp);
        tmp.set_slice(tmpstr);
        pchunk->columns()[1]->append_datum(tmp);
        tmp.set_int32(i * 2343);
        pchunk->columns()[2]->append_datum(tmp);
        tmp.set_uint8(i % 2);
        pchunk->columns()[3]->append_datum(tmp);
    }
    //encode
    auto row_encoder = RowStoreEncoderFactory::instance()->get_or_create_encoder(SIMPLE);
    auto full_row_col = std::make_unique<BinaryColumn>();
    row_encoder->encode_chunk_to_full_row_column(*schema, *pchunk, full_row_col.get());
    std::cout << "encode end" << ", full_row_col" << full_row_col->debug_string()  << std::endl;

    //decode
    std::vector<ColumnId> value_column_ids {2,3};
    auto read_value_schema = std::make_unique<Schema>(schema_with_row.get(), value_column_ids);
    auto resolve_full_row_col = std::make_unique<BinaryColumn>();
    std::vector<std::unique_ptr<Column>> read_value_columns(value_column_ids.size());
    for (uint32_t i = 0; i < value_column_ids.size(); ++i) {
        read_value_columns[i] = ChunkHelper::column_from_field(*((*read_value_schema).field(i)))->clone_empty();
    }
    row_encoder->decode_columns_from_full_row_column(*schema_with_row, *resolve_full_row_col, value_column_ids, read_value_columns);
    std::cout << "dencode end" << std::endl;

    for (int i = 0; i < n; i++) {
        std::cout << "debug" << read_value_columns[0]->debug_string() << std::endl;
        ASSERT_EQ((*(read_value_columns[0])).get(i).get_int32(), i * 2343);
        std::cout << "i = 0" << i << std::endl;
        ASSERT_EQ((*(read_value_columns[1])).get(i).get_uint8(), i % 2);
        std::cout << "i = 1" << i << std::endl;
    }

}

} // namespace starrocks
