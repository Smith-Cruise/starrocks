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

#include "exec/pipeline/scan/chunk_buffer_limiter.h"
#include "exec/pipeline/scan/scan_operator.h"
#include "exec/scan_node.h"
#include "exec/pipeline/scan/datacache_warmup_operator.h"
#include "exec/pipeline/pipeline_builder.h"

namespace starrocks {

class DatacacheWarmupNode final : public ScanNode {
public:
    DatacacheWarmupNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
            : ScanNode(pool, tnode, descs) {}

    Status open(starrocks::RuntimeState *state) override {
        std::cout << "none-pipeline hello world" << std::endl;
        return Status::OK();
    }

    Status get_next(starrocks::RuntimeState *state, starrocks::ChunkPtr *chunk, bool *eos) override {
        std::cout << "none-pipeline get next" << std::endl;
        return Status::OK();
    }

    Status set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) {
        for(const auto&  param: scan_ranges) {
            TScanRange scan_range = param.scan_range;
            std::cout << "none-pipeline" << scan_range.hdfs_scan_range.relative_path << std::endl;
        }
        return Status::OK();
    }

    OpFactories decompose_to_pipeline(pipeline::PipelineBuilderContext *context) override {
        std::cout << "decompose pipeline dop" << std::endl;
        size_t dop = 1;

        size_t buffer_capacity = pipeline::ScanOperator::max_buffer_capacity() * dop;
        pipeline::ChunkBufferLimiterPtr buffer_limiter = std::make_unique<pipeline::DynamicChunkBufferLimiter>(
                buffer_capacity, buffer_capacity, _mem_limit, runtime_state()->chunk_size());

        auto op_factory = std::make_shared<pipeline::DatacacheWarmupOperatorFactory>(context->next_operator_id(), this, dop, std::move(buffer_limiter));
        return pipeline::decompose_scan_node_to_pipeline(op_factory, this, context);
    }
};

} // namespace starrocks