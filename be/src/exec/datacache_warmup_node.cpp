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

#include "datacache_warmup_node.h"

namespace starrocks {

Status DataCacheWarmupNode::open(starrocks::RuntimeState* state) {
    return Status::NotSupported("DataCache warmup only support pipeline-engine in open()");
}

Status DataCacheWarmupNode::get_next(starrocks::RuntimeState* state, starrocks::ChunkPtr* chunk, bool* eos) {
    return Status::NotSupported("DataCache warmup only support pipeline-engine in get_next()");
}

Status DataCacheWarmupNode::set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) {
    return Status::NotSupported("DataCache warmup only support pipeline-engine in set_scan_ranges()");
}

OpFactories DataCacheWarmupNode::decompose_to_pipeline(pipeline::PipelineBuilderContext* context) {
    std::cout << "decompose pipeline dop" << std::endl;
    size_t dop = 1;
    size_t buffer_capacity = pipeline::ScanOperator::max_buffer_capacity() * dop;
    pipeline::ChunkBufferLimiterPtr buffer_limiter = std::make_unique<pipeline::DynamicChunkBufferLimiter>(
            buffer_capacity, buffer_capacity, _mem_limit, runtime_state()->chunk_size());

    auto op_factory = std::make_shared<pipeline::DatacacheWarmupOperatorFactory>(context->next_operator_id(), this, dop, std::move(buffer_limiter));
    return pipeline::decompose_scan_node_to_pipeline(op_factory, this, context);
}


} // namespace starrocks