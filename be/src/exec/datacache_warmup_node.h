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

class DataCacheWarmupNode final : public ScanNode {
public:
    DataCacheWarmupNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
            : ScanNode(pool, tnode, descs) {}

    Status open(starrocks::RuntimeState *state) override;

    Status get_next(starrocks::RuntimeState *state, starrocks::ChunkPtr *chunk, bool *eos) override;

    Status set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) override;

    OpFactories decompose_to_pipeline(pipeline::PipelineBuilderContext *context) override;
};

} // namespace starrocks