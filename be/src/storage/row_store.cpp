// This file is made available under Elastic License 2.0.

#include "storage/row_store.h"

#include "rocksdb/filter_policy.h"
#include "rocksdb/table.h"

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "storage/olap_define.h"
#include "storage/rocksdb_status_adapter.h"

using rocksdb::DB;
using rocksdb::Options;
using rocksdb::WriteOptions;

namespace starrocks {

RowStore::RowStore(std::string db_path) : _db_path(std::move(db_path)) {}

RowStore::~RowStore() {
    if (_db != nullptr) {
        _db->Close();
        delete _db;
        _db = nullptr;
    }
}

Status RowStore::init() {
    Options options;
    options.IncreaseParallelism();
    options.create_if_missing = true;
    options.prefix_extractor.reset(rocksdb::NewCappedPrefixTransform(key_prefix_len_));
    // Enable prefix bloom for mem tables
    options.memtable_prefix_bloom_size_ratio = 0.1;
    // Setup bloom filter
    rocksdb::BlockBasedTableOptions table_options;
    table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
    table_options.whole_key_filtering = false;
    options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

    // open rocksdb
    rocksdb::Status s = DB::Open(options, _db_path, &_db);
    if (!s.ok()) {
        return to_status(s);
    }
    if (_db == nullptr) {
        return Status::InternalError("RowStore rocksdb open failed");
    }
    return Status::OK();
}

Status RowStore::batch_put(const std::vector<std::string>& keys, const std::vector<std::string>& values,
                           int64_t version) {
    CHECK(keys.size() == values.size()) << "rowstore batch_put invalid kv pairs";
    rocksdb::WriteBatch wb;
    for (int i = 0; i < keys.size(); i++) {
        wb.Put(keys[i], values[i]);
    }
    WriteOptions write_options;
    write_options.sync = true;
    rocksdb::Status s = _db->Write(write_options, &wb);
    return to_status(s);
}

} // namespace starrocks