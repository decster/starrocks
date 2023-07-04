#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "common/status.h"
#include "gutil/endian.h"
#include "storage/chunk_helper.h"
#include "storage/olap_common.h"
#include "storage/tablet_schema.h"
#include "types/date_value.hpp"
#include "storage/primary_key_encoder.h"
#include "storage/row_store_encoder_length.h"
#include "storage/row_store_encoder_util.h"

namespace starrocks {

// length + value
inline Status encode_length_slice(const Slice& s, string* dst, size_t col_length) {
    int32_t len = s.size;
    if (col_length > 0 && len > col_length) {
        return Status::DataQualityError("String length exceede the char length define.");
    }
    encode_integral<int32_t>(len, dst);
    dst->append(s.data, len);
    return Status::OK();
}

inline Status decode_length_slice(Slice* src, std::string* dest) {
    int32_t len;
    decode_integral<int32_t>(src, &len);
    dest->append(src->data, len);
    src->remove_prefix(len);
    return Status::OK();
}

// encode version
void encode_header(int32_t col_length, std::string* dest) {
    encode_integral<int32_t>(ROW_STORE_VERSION, dest);
    encode_integral<int32_t>(col_length, dest);
}
// decode version
void decode_header(Slice* src, int32_t* version, int32_t& num_value_cols) {
    decode_integral<int32_t>(src, version);
    decode_integral<int32_t>(src, &num_value_cols);
}

// encode bitmap<column_num>
inline void encode_null_bitmap(BitmapValue& null_bitmap, std::string* dest) {
    size_t len = null_bitmap.getSizeInBytes();
    encode_integral<int32_t>(len, dest);
    null_bitmap.write(const_cast<char *>(dest->c_str()));
}

// decode bitma<column_num>
inline void decode_null_bitmap(Slice* src, BitmapValue& null_bitmap) {
    // current use slice seperator, it's easy, read slice function has already existed
    std::string dest;
    int32_t len = 0;
    decode_integral<int32_t>(src, &len);
    Slice null_bitmap_slice(src->get_data(), len);
    null_bitmap.deserialize(null_bitmap_slice.get_data());
    src->remove_prefix(len);
}

// encode offset
inline void encode_offset(int32_t length, std::string* dest) {
    encode_integral<int32_t>(length, dest);
}

typedef std::pair<int32_t, int32_t> Range;
// decode offset
inline void decode_offset(Slice* src, std::vector<Range>* ranges, const int32_t& num_value_cols) {
    int32_t start_offset = 0;
    int32_t end_offset = 0;
    for (int i = 0; i < num_value_cols; i++) {
        decode_integral<int32_t>(src, &end_offset);
        Range range(start_offset, end_offset);
        ranges->emplace_back(std::move(range));
        start_offset = end_offset;
    }
}

Status RowStoreEncoderLength::encode_chunk_to_full_row_column(const Schema& schema, const Chunk& chunk,
                                                            BinaryColumn* dest_column) {
    std::vector<EncodeOp> ops;
    std::vector<const void*> datas;
    int num_rows = chunk.num_rows();
    int num_cols = schema.num_fields();
    int num_key_cols = schema.num_key_fields();

    prepare_ops_datas_selective(schema, num_key_cols, num_cols - 1, &datas, chunk, &ops);
    dest_column->reserve(dest_column->size() + num_rows);

    std::string header;
    std::string null_bitmap_str;
    std::string buff;
    BitmapValue null_bitmap;
    for (size_t i = 0; i < num_rows; i++) {
        header.clear();
        null_bitmap.clear();
        null_bitmap_str.clear();
        buff.clear();
        // header
        encode_header(num_cols, &header);
        // bitset + value
        for (int j = 0; j < num_cols; j++) {
            if (chunk.columns()[j]->get(i).is_null()) {
                if (schema.field(j)->is_nullable()) {
                    null_bitmap.add(j);
                } else {
                    return Status::InternalError("Null vale in non-null filed.");
                }
            } else {
                ops[j](datas[j], i, &buff);
            }
        }
        encode_null_bitmap(null_bitmap, &null_bitmap_str);
        std::stringstream ss;
        ss << header << null_bitmap_str << buff;
        dest_column->append(buff);
    }
    return Status::OK();
}

Status RowStoreEncoderLength::decode_columns_from_full_row_column(const Schema& schema,
                                                                 const BinaryColumn& full_row_column,
                                                                 const std::vector<uint32_t>& read_column_ids,
                                                                 std::vector<std::unique_ptr<Column>>& dest) {
    int num_rows = full_row_column.size();
    for (size_t i = 0; i < num_rows; i++) {
        Slice s = full_row_column.get_slice(i);
        int32_t version = ROW_STORE_VERSION;
        
        size_t num_cols = schema.num_fields();
        size_t num_key_cols = schema.num_key_fields();
        int32_t num_value_cols = num_cols - num_key_cols;

        // header
        decode_header(&s, &version, num_value_cols);
        BitmapValue null_bitmap;
        // null bitset
        decode_null_bitmap(&s, null_bitmap);
        // ranges
        std::vector<Range> ranges;
        decode_offset(&s, &ranges, num_value_cols);

        //value
        uint32_t cur_read_idx = 0;
        for (uint j = schema.num_key_fields(); j <= read_column_ids.back(); j++) {
            Column* dest_column = nullptr;
            if (read_column_ids[cur_read_idx] == j) {
                dest_column = dest[cur_read_idx].get();
                cur_read_idx++;
                if (null_bitmap.contains(j)) {
                    dest_column->append_nulls(1);
                    continue;
                }
            } else {
                // skip not read fields
                int length = ranges[j].second - ranges[j].first;
                s.remove_prefix(length);
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
                std::string v;
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

void RowStoreEncoderLength::variable_slice_encode(std::vector<EncodeOp>* pops, int col_id, bool is_last) {
    auto& ops = *pops;
    ops[col_id] = [](const void* data, int idx, string* buff) {
        auto data_ptr = ((const Slice*)data)[idx];
        size_t col_length = data_ptr.size;
        encode_length_slice(data_ptr, buff, col_length);
    };
}

Status RowStoreEncoderLength::variable_slice_decode(Slice* src, std::string* dest, bool is_last) {
    return decode_length_slice(src, dest);
}

} // namespace starrocks