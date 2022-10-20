
// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/row_store_encoder.h"

#include <gtest/gtest.h>

#include "column/chunk.h"
#include "column/datum.h"
#include "column/schema.h"
#include "storage/chunk_helper.h"

using namespace std;

namespace starrocks {

static unique_ptr<vectorized::Schema> create_schema(const vector<pair<FieldType, bool>>& types) {
    vectorized::Fields fields;
    for (int i = 0; i < types.size(); i++) {
        string name = StringPrintf("col%d", i);
        auto fd = new vectorized::Field(i, name, types[i].first, false);
        fd->set_is_key(types[i].second);
        fd->set_aggregate_method(OLAP_FIELD_AGGREGATION_NONE);
        fields.emplace_back(fd);
    }
    return unique_ptr<vectorized::Schema>(new vectorized::Schema(std::move(fields)));
}

TEST(RowStoreEncoderTest, testEncodeInt) {
    auto schema = create_schema({{OLAP_FIELD_TYPE_INT, true},
                                 {OLAP_FIELD_TYPE_LARGEINT, true},
                                 {OLAP_FIELD_TYPE_INT, false},
                                 {OLAP_FIELD_TYPE_LARGEINT, false}});
    const int n = 100;
    auto pchunk = ChunkHelper::new_chunk(*schema, n);
    for (int i = 0; i < n; i++) {
        vectorized::Datum tmp;
        tmp.set_int32(i * 2343);
        vectorized::Datum tmp2;
        tmp2.set_int128(i * 2343);
        pchunk->columns()[0]->append_datum(tmp);
        pchunk->columns()[1]->append_datum(tmp2);
        pchunk->columns()[2]->append_datum(tmp);
        pchunk->columns()[3]->append_datum(tmp2);
    }
    std::vector<std::string> keys, values;
    RowStoreEncoder::chunk_to_keys(*schema, *pchunk, 0, n, keys);
    RowStoreEncoder::chunk_to_values(*schema, *pchunk, 0, n, values);
    ASSERT_EQ(keys.size(), n);
    ASSERT_EQ(values.size(), n);
    auto dchunk = pchunk->clone_empty_with_schema();
    RowStoreEncoder::kvs_to_chunk(keys, values, *schema, dchunk.get());
    ASSERT_EQ(pchunk->num_rows(), dchunk->num_rows());
    ASSERT_EQ(pchunk->num_rows(), n);
    ASSERT_EQ(pchunk->num_columns(), 4);
    ASSERT_EQ(dchunk->num_columns(), 4);
    for (int i = 0; i < n; i++) {
        ASSERT_EQ(pchunk->get_column_by_index(0)->get(i).get_int32(),
                  dchunk->get_column_by_index(0)->get(i).get_int32());
        ASSERT_EQ(pchunk->get_column_by_index(1)->get(i).get_int128(),
                  dchunk->get_column_by_index(1)->get(i).get_int128());
        ASSERT_EQ(pchunk->get_column_by_index(2)->get(i).get_int32(),
                  dchunk->get_column_by_index(2)->get(i).get_int32());
        ASSERT_EQ(pchunk->get_column_by_index(3)->get(i).get_int128(),
                  dchunk->get_column_by_index(3)->get(i).get_int128());
    }
}

TEST(RowStoreEncoderTest, testEncodeMix) {
    auto schema = create_schema({{OLAP_FIELD_TYPE_INT, true},
                                 {OLAP_FIELD_TYPE_VARCHAR, true},
                                 {OLAP_FIELD_TYPE_INT, false},
                                 {OLAP_FIELD_TYPE_BOOL, false}});
    const int n = 100;
    auto pchunk = ChunkHelper::new_chunk(*schema, n);
    for (int i = 0; i < n; i++) {
        vectorized::Datum tmp;
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
    std::vector<std::string> keys, values;
    RowStoreEncoder::chunk_to_keys(*schema, *pchunk, 0, n, keys);
    RowStoreEncoder::chunk_to_values(*schema, *pchunk, 0, n, values);
    ASSERT_EQ(keys.size(), n);
    ASSERT_EQ(values.size(), n);
    auto dchunk = pchunk->clone_empty_with_schema();
    RowStoreEncoder::kvs_to_chunk(keys, values, *schema, dchunk.get());
    ASSERT_EQ(pchunk->num_rows(), dchunk->num_rows());
    ASSERT_EQ(pchunk->num_rows(), n);
    ASSERT_EQ(pchunk->num_columns(), 4);
    ASSERT_EQ(dchunk->num_columns(), 4);
    for (int i = 0; i < n; i++) {
        ASSERT_EQ(pchunk->get_column_by_index(0)->get(i).get_int32(),
                  dchunk->get_column_by_index(0)->get(i).get_int32());
        ASSERT_EQ(pchunk->get_column_by_index(1)->get(i).get_slice(),
                  dchunk->get_column_by_index(1)->get(i).get_slice());
        ASSERT_EQ(pchunk->get_column_by_index(2)->get(i).get_int32(),
                  dchunk->get_column_by_index(2)->get(i).get_int32());
        ASSERT_EQ(pchunk->get_column_by_index(3)->get(i).get<uint8>(),
                  dchunk->get_column_by_index(3)->get(i).get<uint8>());
    }
}

} // namespace starrocks