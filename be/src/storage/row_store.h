// This file is made available under Elastic License 2.0.

#pragma once

#include <rocksdb/write_batch.h>

#include <functional>
#include <map>
#include <string>
#include <string_view>

#include "common/status.h"
#include "storage/olap_common.h"

namespace rocksdb {
class DB;
class ColumnFamilyHandle;
} // namespace rocksdb

namespace starrocks {

using ColumnFamilyHandle = rocksdb::ColumnFamilyHandle;
using WriteBatch = rocksdb::WriteBatch;

class RowStore {
public:
    explicit RowStore(std::string db_path);

    virtual ~RowStore();

    Status init();

    // write key value pairs to rowstore with version
    Status batch_put(const std::vector<std::string>& keys, const std::vector<std::string>& values, int64_t version);

private:
    std::string _db_path;
    rocksdb::DB* _db;
    uint32_t key_prefix_len_{10};
};

} // namespace starrocks