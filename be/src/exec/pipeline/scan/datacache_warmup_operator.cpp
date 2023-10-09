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

#include "exec/pipeline/scan/datacache_warmup_operator.h"

namespace starrocks::pipeline {

OperatorPtr DatacacheWarmupOperatorFactory::do_create(int32_t dop, int32_t driver_sequence) {
    return std::make_shared<DatacacheWarmupOperator>(this, _id, driver_sequence, dop, _scan_node);
}

DatacacheWarmupOperator::DatacacheWarmupOperator(DatacacheWarmupOperatorFactory* factory, int32_t id, int32_t driver_sequence, int32_t dop,
                        ScanNode* scan_node) : ScanOperator(factory, id, driver_sequence, dop, scan_node), _chunk_buffer(factory->get_chunk_buffer()) {
}

ChunkSourcePtr DatacacheWarmupOperator::create_chunk_source(starrocks::pipeline::MorselPtr morsel, int32_t chunk_source_index) {
    return std::make_shared<DatacacheWarmupChunkSource>(this, _chunk_source_profiles[chunk_source_index].get(), std::move(morsel), _chunk_buffer);
}


} // namespace starrocks::pipeline