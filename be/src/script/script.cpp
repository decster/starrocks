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

#include "script/script.h"

#include "common/logging.h"
#include "exec/schema_scanner/schema_be_tablets_scanner.h"
#include "gutil/strings/substitute.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"
#include "storage/tablet_manager.h"
#include "wrenbind17/wrenbind17.hpp"

using namespace wrenbind17;
using std::string;

namespace starrocks {

#define REG_VAR(TYPE, NAME) cls.var<&TYPE::NAME>(#NAME)
#define REG_METHOD(TYPE, NAME) cls.func<&TYPE::NAME>(#NAME)
#define REG_STATIC_METHOD(TYPE, NAME) cls.funcStatic<&TYPE::NAME>(#NAME)

static void bind_common(ForeignModule& m) {
    {
        auto& cls = m.klass<Status>("Status");
        cls.func<&Status::to_string>("toString");
        REG_METHOD(Status, ok);
    }
}

class StorageEngineRef {
public:
    static string drop_tablet(int64_t tablet_id) {
        auto manager = StorageEngine::instance()->tablet_manager();
        string err;
        auto ptr = manager->get_tablet(tablet_id, true, &err);
        if (ptr == nullptr) {
            return strings::Substitute("get tablet $0 failed: $1", tablet_id, err);
        }
        auto st = manager->drop_tablet(tablet_id, TabletDropFlag::kKeepMetaAndFiles);
        return st.to_string();
    }

    static std::shared_ptr<Tablet> get_tablet(int64_t tablet_id) {
        string err;
        auto ptr = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, true, &err);
        if (ptr == nullptr) {
            LOG(WARNING) << "get_tablet " << tablet_id << " failed: " << err;
            return nullptr;
        }
        return ptr;
    }

    static std::shared_ptr<TabletBasicInfo> get_tablet_info(int64_t tablet_id) {
        std::vector<TabletBasicInfo> tablet_infos;
        auto manager = StorageEngine::instance()->tablet_manager();
        manager->get_tablets_basic_infos(-1, -1, tablet_id, tablet_infos);
        if (tablet_infos.empty()) {
            return nullptr;
        } else {
            return std::make_shared<TabletBasicInfo>(tablet_infos[0]);
        }
    }

    static std::vector<TabletBasicInfo> get_tablet_infos(int64_t table_id, int64_t partition_id) {
        std::vector<TabletBasicInfo> tablet_infos;
        auto manager = StorageEngine::instance()->tablet_manager();
        manager->get_tablets_basic_infos(table_id, partition_id, -1, tablet_infos);
        return tablet_infos;
    }

    static void bind(ForeignModule& m) {
        {
            auto& cls = m.klass<TabletBasicInfo>("TabletBasicInfo");
            REG_VAR(TabletBasicInfo, table_id);
            REG_VAR(TabletBasicInfo, partition_id);
            REG_VAR(TabletBasicInfo, tablet_id);
            REG_VAR(TabletBasicInfo, num_version);
            REG_VAR(TabletBasicInfo, max_version);
            REG_VAR(TabletBasicInfo, min_version);
            REG_VAR(TabletBasicInfo, num_rowset);
            REG_VAR(TabletBasicInfo, num_row);
            REG_VAR(TabletBasicInfo, data_size);
            REG_VAR(TabletBasicInfo, index_mem);
            REG_VAR(TabletBasicInfo, create_time);
            REG_VAR(TabletBasicInfo, state);
            REG_VAR(TabletBasicInfo, type);
        }
        {
            auto& cls = m.klass<Tablet>("Tablet");
            REG_METHOD(Tablet, tablet_id);
            REG_METHOD(Tablet, tablet_footprint);
            REG_METHOD(Tablet, num_rows);
            REG_METHOD(Tablet, version_count);
            REG_METHOD(Tablet, max_version);
            REG_METHOD(Tablet, max_continuous_version);
            REG_METHOD(Tablet, compaction_score);
            REG_METHOD(Tablet, schema_debug_string);
            REG_METHOD(Tablet, debug_string);
            REG_METHOD(Tablet, support_binlog);
        }
        {
            auto& cls = m.klass<DataDir>("DataDir");
            REG_METHOD(DataDir, path);
            REG_METHOD(DataDir, path_hash);
            REG_METHOD(DataDir, is_used);
            REG_METHOD(DataDir, get_meta);
            REG_METHOD(DataDir, is_used);
            REG_METHOD(DataDir, available_bytes);
            REG_METHOD(DataDir, disk_capacity_bytes);
        }
        {
            auto& cls = m.klass<KVStore>("KVStore");
            REG_METHOD(KVStore, compact);
            REG_METHOD(KVStore, flushMemTable);
            REG_METHOD(KVStore, get_stats);
        }
        {
            auto& cls = m.klass<StorageEngineRef>("StorageEngine");
            REG_STATIC_METHOD(StorageEngineRef, get_tablet_info);
            REG_STATIC_METHOD(StorageEngineRef, get_tablet_infos);
            REG_STATIC_METHOD(StorageEngineRef, get_tablet);
            REG_STATIC_METHOD(StorageEngineRef, drop_tablet);
        }
    }
};

Status execute_script(const std::string& script, std::string& output) {
    wrenbind17::VM vm;
    vm.setPrintFunc([&](const char* text) { output.append(text); });
    auto& m = vm.module("starrocks");
    bind_common(m);
    StorageEngineRef::bind(m);
    vm.runFromSource("main", R"(import "starrocks" for TabletBasicInfo, Tablet, StorageEngine)");
    try {
        vm.runFromSource("main", script);
    } catch (const std::exception& e) {
        output.append(e.what());
    }
    return Status::OK();
}

} // namespace starrocks