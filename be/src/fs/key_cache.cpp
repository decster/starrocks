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

#include "fs/key_cache.h"

#include "agent/master_info.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/Types_types.h"
#include "runtime/client_cache.h"
#include "util/thrift_rpc_helper.h"

namespace starrocks {

StatusOr<std::unique_ptr<EncryptionKey>> EncryptionKey::create_from_pb(const EncryptionKeyPB& pb) {}

StatusOr<FileEncryptionPair> KeyCache::create_encryption_pair_for_new_file() {
    FileEncryptionPair ret;
    // TODO:
    return ret;
}

StatusOr<FileEncryptionInfo> KeyCache::unwrap_from_meta(const std::vector<uint8_t>& encryption_meta) {
    FileEncryptionInfo ret;
    // TODO:
    return ret;
}

Status KeyCache::refresh_keys_from_fe() {
    TNetworkAddress master_addr = get_master_address();
    TGetKeysRequest req;
    TGetKeysResponse resp;
    RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&req, &resp](FrontendServiceConnection& client) { client->getKeys(resp, req); }));
    return refresh_keys(resp.key_metas);
}

Status KeyCache::refresh_keys(const std::vector<std::string>& key_metas) {
    for (auto& key_meta : key_metas) {
        EncryptionKeyPB pb;
        if (!pb.ParseFromString(key_meta)) {
            return Status::Corruption("bad encryption key pb");
        }
        RETURN_IF_ERROR(add_key(pb));
    }
    return Status::OK();
}

Status KeyCache::add_key(const EncryptionKeyPB& pb) {
    std::lock_guard lg(_lock);
    auto itr = _id_to_keys.find(pb.id());
    if (itr == _id_to_keys.end()) {
        ASSIGN_OR_RETURN(auto key, EncryptionKey::create_from_pb(pb));
        _id_to_keys.insert({pb.id(), std::move(key)});
    }
}

} // namespace starrocks
