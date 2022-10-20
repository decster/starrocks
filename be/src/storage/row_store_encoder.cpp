// This file is made available under Elastic License 2.0.

#include "storage/row_store_encoder.h"

#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "gutil/endian.h"
#include "storage/tablet_schema.h"
#include "types/date_value.hpp"

namespace starrocks {

template <class UT>
UT _to_bigendian(UT v);

template <>
uint8_t _to_bigendian(uint8_t v) {
    return v;
}
template <>
uint16_t _to_bigendian(uint16_t v) {
    return BigEndian::FromHost16(v);
}
template <>
uint32_t _to_bigendian(uint32_t v) {
    return BigEndian::FromHost32(v);
}
template <>
uint64_t _to_bigendian(uint64_t v) {
    return BigEndian::FromHost64(v);
}
template <>
uint128_t _to_bigendian(uint128_t v) {
    return BigEndian::FromHost128(v);
}

template <class T>
void encode_integral(const T& v, string* dest) {
    if constexpr (std::is_signed<T>::value) {
        typedef typename std::make_unsigned<T>::type UT;
        UT uv = v;
        uv ^= static_cast<UT>(1) << (sizeof(UT) * 8 - 1);
        uv = _to_bigendian(uv);
        dest->append(reinterpret_cast<const char*>(&uv), sizeof(uv));
    } else {
        T nv = _to_bigendian(v);
        dest->append(reinterpret_cast<const char*>(&nv), sizeof(nv));
    }
}

template <class T>
void decode_integral(Slice* src, T* v) {
    if constexpr (std::is_signed<T>::value) {
        typedef typename std::make_unsigned<T>::type UT;
        UT uv = *(UT*)(src->data);
        uv = _to_bigendian(uv);
        uv ^= static_cast<UT>(1) << (sizeof(UT) * 8 - 1);
        *v = uv;
    } else {
        T nv = *(T*)(src->data);
        *v = _to_bigendian(nv);
    }
    src->remove_prefix(sizeof(T));
}

template <int LEN>
static bool SSEEncodeChunk(const uint8_t** srcp, uint8_t** dstp) {
#if defined(__aarch64__) || !defined(__SSE4_2__)
    return false;
#else
    __m128i data;
    if (LEN == 16) {
        // Load 16 bytes (unaligned) into the XMM register.
        data = _mm_loadu_si128(reinterpret_cast<const __m128i*>(*srcp));
    } else if (LEN == 8) {
        // Load 8 bytes (unaligned) into the XMM register
        data = reinterpret_cast<__m128i>(_mm_load_sd(reinterpret_cast<const double*>(*srcp)));
    }
    // Compare each byte of the input with '\0'. This results in a vector
    // where each byte is either \x00 or \xFF, depending on whether the
    // input had a '\x00' in the corresponding position.
    __m128i zeros = reinterpret_cast<__m128i>(_mm_setzero_pd());
    __m128i zero_bytes = _mm_cmpeq_epi8(data, zeros);

    // Check whether the resulting vector is all-zero.
    bool all_zeros;
    if (LEN == 16) {
        all_zeros = _mm_testz_si128(zero_bytes, zero_bytes);
    } else { // LEN == 8
        all_zeros = _mm_cvtsi128_si64(zero_bytes) == 0;
    }

    // If it's all zero, we can just store the entire chunk.
    if (PREDICT_FALSE(!all_zeros)) {
        return false;
    }

    if (LEN == 16) {
        _mm_storeu_si128(reinterpret_cast<__m128i*>(*dstp), data);
    } else {
        _mm_storel_epi64(reinterpret_cast<__m128i*>(*dstp), data); // movq m64, xmm
    }
    *dstp += LEN;
    *srcp += LEN;
    return true;
#endif //__aarch64__
}

// Non-SSE loop which encodes 'len' bytes from 'srcp' into 'dst'.
static inline void EncodeChunkLoop(const uint8_t** srcp, uint8_t** dstp, int len) {
    while (len--) {
        if (PREDICT_FALSE(**srcp == '\0')) {
            *(*dstp)++ = 0;
            *(*dstp)++ = 1;
        } else {
            *(*dstp)++ = **srcp;
        }
        (*srcp)++;
    }
}

inline void encode_slice(const Slice& s, string* dst, bool is_last) {
    if (is_last) {
        dst->append(s.data, s.size);
    } else {
        // If we're a middle component of a composite key, we need to add a \x00
        // at the end in order to separate this component from the next one. However,
        // if we just did that, we'd have issues where a key that actually has
        // \x00 in it would compare wrong, so we have to instead add \x00\x00, and
        // encode \x00 as \x00\x01.
        size_t old_size = dst->size();
        dst->resize(old_size + s.size * 2 + 2);

        const uint8_t* srcp = (const uint8_t*)s.data;
        uint8_t* dstp = reinterpret_cast<uint8_t*>(&(*dst)[old_size]);
        size_t len = s.size;
        size_t rem = len;

        while (rem >= 16) {
            if (!SSEEncodeChunk<16>(&srcp, &dstp)) {
                goto slow_path;
            }
            rem -= 16;
        }
        while (rem >= 8) {
            if (!SSEEncodeChunk<8>(&srcp, &dstp)) {
                goto slow_path;
            }
            rem -= 8;
        }
        // Roll back to operate in 8 bytes at a time.
        if (len > 8 && rem > 0) {
            dstp -= 8 - rem;
            srcp -= 8 - rem;
            if (!SSEEncodeChunk<8>(&srcp, &dstp)) {
                // TODO: optimize for the case where the input slice has '\0'
                // bytes. (e.g. move the pointer to the first zero byte.)
                dstp += 8 - rem;
                srcp += 8 - rem;
                goto slow_path;
            }
            rem = 0;
            goto done;
        }

    slow_path:
        EncodeChunkLoop(&srcp, &dstp, rem);

    done:
        *dstp++ = 0;
        *dstp++ = 0;
        dst->resize(dstp - reinterpret_cast<uint8_t*>(&(*dst)[0]));
    }
}

inline Status decode_slice(Slice* src, string* dest, bool is_last) {
    if (is_last) {
        dest->append(src->data, src->size);
    } else {
        uint8_t* separator = static_cast<uint8_t*>(memmem(src->data, src->size, "\0\0", 2));
        DCHECK(separator) << "bad encoded primary key, separator not found";
        if (PREDICT_FALSE(separator == nullptr)) {
            LOG(WARNING) << "bad encoded primary key, separator not found";
            return Status::InvalidArgument("bad encoded primary key, separator not found");
        }
        uint8_t* data = (uint8_t*)src->data;
        int len = separator - data;
        for (int i = 0; i < len; i++) {
            if (i >= 1 && data[i - 1] == '\0' && data[i] == '\1') {
                continue;
            }
            dest->push_back((char)data[i]);
        }
        src->remove_prefix(len + 2);
    }
    return Status::OK();
}

typedef void (*EncodeOp)(const void*, int, string*);

static void prepare_ops_datas_selective(const vectorized::Schema& schema, const vectorized::Chunk& chunk,
                                        vector<EncodeOp>* pops, int from_col, int to_col, vector<const void*>* pdatas) {
    CHECK(from_col <= to_col && from_col >= 0 && to_col < schema.num_fields()) << "invalid input col index";
    auto& ops = *pops;
    auto& datas = *pdatas;
    ops.resize(to_col + 1, nullptr);
    datas.resize(to_col + 1, nullptr);
    for (int j = from_col; j <= to_col; j++) {
        datas[j] = chunk.get_column_by_index(j)->raw_data();
        switch (schema.field(j)->type()->type()) {
        case OLAP_FIELD_TYPE_BOOL:
            ops[j] = [](const void* data, int idx, string* buff) {
                encode_integral(((const uint8_t*)data)[idx], buff);
            };
            break;
        case OLAP_FIELD_TYPE_TINYINT:
            ops[j] = [](const void* data, int idx, string* buff) { encode_integral(((const int8_t*)data)[idx], buff); };
            break;
        case OLAP_FIELD_TYPE_SMALLINT:
            ops[j] = [](const void* data, int idx, string* buff) {
                encode_integral(((const int16_t*)data)[idx], buff);
            };
            break;
        case OLAP_FIELD_TYPE_INT:
            ops[j] = [](const void* data, int idx, string* buff) {
                encode_integral(((const int32_t*)data)[idx], buff);
            };
            break;
        case OLAP_FIELD_TYPE_BIGINT:
            ops[j] = [](const void* data, int idx, string* buff) {
                encode_integral(((const int64_t*)data)[idx], buff);
            };
            break;
        case OLAP_FIELD_TYPE_LARGEINT:
            ops[j] = [](const void* data, int idx, string* buff) {
                encode_integral(((const int128_t*)data)[idx], buff);
            };
            break;
        case OLAP_FIELD_TYPE_VARCHAR:
            if (j == to_col) {
                ops[j] = [](const void* data, int idx, string* buff) {
                    encode_slice(((const Slice*)data)[idx], buff, true);
                };
            } else {
                ops[j] = [](const void* data, int idx, string* buff) {
                    encode_slice(((const Slice*)data)[idx], buff, false);
                };
            }
            break;
        case OLAP_FIELD_TYPE_DATE_V2:
            ops[j] = [](const void* data, int idx, string* buff) {
                encode_integral(((const int32_t*)data)[idx], buff);
            };
            break;
        case OLAP_FIELD_TYPE_TIMESTAMP:
            ops[j] = [](const void* data, int idx, string* buff) {
                encode_integral(((const int64_t*)data)[idx], buff);
            };
            break;
        default:
            CHECK(false) << "type not supported for primary key encoding "
                         << field_type_to_string(schema.field(j)->type()->type());
        }
    }
}

void RowStoreEncoder::chunk_to_keys(const vectorized::Schema& schema, const vectorized::Chunk& chunk, size_t offset,
                                    size_t len, std::vector<std::string>& keys) {
    vector<EncodeOp> ops;
    vector<const void*> datas;
    prepare_ops_datas_selective(schema, chunk, &ops, 0, schema.num_key_fields() - 1, &datas);
    keys.resize(len);
    for (size_t i = 0; i < len; i++) {
        for (int j = 0; j < schema.num_key_fields(); j++) {
            ops[j](datas[j], offset + i, &keys[i]);
        }
    }
}

void RowStoreEncoder::chunk_to_values(const vectorized::Schema& schema, const vectorized::Chunk& chunk, size_t offset,
                                      size_t len, std::vector<std::string>& values) {
    vector<EncodeOp> ops;
    vector<const void*> datas;
    prepare_ops_datas_selective(schema, chunk, &ops, schema.num_key_fields(), schema.num_fields() - 1, &datas);
    values.resize(len);
    for (size_t i = 0; i < len; i++) {
        for (int j = schema.num_key_fields(); j < schema.num_fields(); j++) {
            ops[j](datas[j], offset + i, &values[i]);
        }
    }
}

Status RowStoreEncoder::kvs_to_chunk(const std::vector<std::string>& keys, const std::vector<std::string>& values,
                                     const vectorized::Schema& schema, vectorized::Chunk* dest) {
    CHECK(keys.size() == values.size()) << "key size should equal to value size";
    for (int i = 0; i < keys.size(); i++) {
        Slice s = Slice(keys[i]);
        for (int j = 0; j < schema.num_fields(); j++) {
            if (j == schema.num_key_fields()) {
                s = Slice(values[i]);
            }
            auto& column = *(dest->get_column_by_index(j));
            switch (schema.field(j)->type()->type()) {
            case OLAP_FIELD_TYPE_BOOL: {
                auto& tc = down_cast<vectorized::UInt8Column&>(column);
                uint8_t v;
                decode_integral(&s, &v);
                tc.append((int8_t)v);
            } break;
            case OLAP_FIELD_TYPE_TINYINT: {
                auto& tc = down_cast<vectorized::Int8Column&>(column);
                int8_t v;
                decode_integral(&s, &v);
                tc.append(v);
            } break;
            case OLAP_FIELD_TYPE_SMALLINT: {
                auto& tc = down_cast<vectorized::Int16Column&>(column);
                int16_t v;
                decode_integral(&s, &v);
                tc.append(v);
            } break;
            case OLAP_FIELD_TYPE_INT: {
                auto& tc = down_cast<vectorized::Int32Column&>(column);
                int32_t v;
                decode_integral(&s, &v);
                tc.append(v);
            } break;
            case OLAP_FIELD_TYPE_BIGINT: {
                auto& tc = down_cast<vectorized::Int64Column&>(column);
                int64_t v;
                decode_integral(&s, &v);
                tc.append(v);
            } break;
            case OLAP_FIELD_TYPE_LARGEINT: {
                auto& tc = down_cast<vectorized::Int128Column&>(column);
                int128_t v;
                decode_integral(&s, &v);
                tc.append(v);
            } break;
            case OLAP_FIELD_TYPE_VARCHAR: {
                auto& tc = down_cast<vectorized::BinaryColumn&>(column);
                string v;
                RETURN_IF_ERROR(
                        decode_slice(&s, &v, (j == schema.num_fields() - 1) || (j == schema.num_key_fields() - 1)));
                tc.append(v);
            } break;
            case OLAP_FIELD_TYPE_DATE_V2: {
                auto& tc = down_cast<vectorized::DateColumn&>(column);
                vectorized::DateValue v;
                decode_integral(&s, &v._julian);
                tc.append(v);
            } break;
            case OLAP_FIELD_TYPE_DATETIME: {
                auto& tc = down_cast<vectorized::TimestampColumn&>(column);
                vectorized::TimestampValue v;
                decode_integral(&s, &v._timestamp);
                tc.append(v);
            } break;
            default:
                CHECK(false) << "type not supported for primary key encoding";
            }
        }
    }
    return Status::OK();
}

} // namespace starrocks