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
    explicit RowStore(std::string db_path, int64_t tablet_id);

    virtual ~RowStore();

private:
};

} // namespace starrocks