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

#pragma once

#include "exec/pipeline/scan/scan_operator.h"
#include "exec/pipeline/scan/balanced_chunk_buffer.h"
#include "exec/workgroup/work_group.h"

namespace starrocks::pipeline {

class DatacacheWarmupOperatorFactory final : public ScanOperatorFactory {
public:
    DatacacheWarmupOperatorFactory(int32_t id, ScanNode* scan_node, size_t dop,
                                   ChunkBufferLimiterPtr buffer_limiter) : ScanOperatorFactory(id, scan_node),
              _chunk_buffer(BalanceStrategy::kDirect, dop, std::move(buffer_limiter)){}

    ~DatacacheWarmupOperatorFactory() override = default;

    bool with_morsels() const override { return true; }

    Status do_prepare(RuntimeState* state) override {
        return Status::OK();
    }
    void do_close(RuntimeState* state) override {
        std::cout << "warmup operator factory close" << std::endl;
    }

    BalancedChunkBuffer& get_chunk_buffer() { return _chunk_buffer; }

    OperatorPtr do_create(int32_t dop, int32_t driver_sequence) override;
private:
    BalancedChunkBuffer _chunk_buffer;
};

class DatacacheWarmupOperator final : public ScanOperator {
public:
    DatacacheWarmupOperator(DatacacheWarmupOperatorFactory* factory, int32_t id, int32_t driver_sequence, int32_t dop,
                            ScanNode* scan_node);

    ~DatacacheWarmupOperator() override = default;

    Status do_prepare(RuntimeState* state) override {
        return Status::OK();
    }
    void do_close(RuntimeState* state) override {
        std::cout << "operator do_close" << std::endl;
    }
    ChunkSourcePtr create_chunk_source(MorselPtr morsel, int32_t chunk_source_index) override;

private:
    void attach_chunk_source(int32_t source_index) override {}
    void detach_chunk_source(int32_t source_index) override {}
    bool has_shared_chunk_source() const override { return false; }
    ChunkPtr get_chunk_from_buffer() override {
        ChunkPtr chunk = nullptr;
        if (_chunk_buffer.try_get(_driver_sequence, &chunk)) {
            return chunk;
        }
        return nullptr;
    }
    size_t num_buffered_chunks() const override {
        return _chunk_buffer.size(_driver_sequence);
    }
    size_t buffer_size() const override {
        return _chunk_buffer.limiter()->size();
    }
    size_t buffer_capacity() const override {
        return _chunk_buffer.limiter()->capacity();
    }
    size_t buffer_memory_usage() const override {
        return _chunk_buffer.memory_usage();
    }
    size_t default_buffer_capacity() const override {
        return _chunk_buffer.limiter()->default_capacity();
    }
    ChunkBufferTokenPtr pin_chunk(int num_chunks) override {
        return _chunk_buffer.limiter()->pin(num_chunks);
    }
    bool is_buffer_full() const override {
        return _chunk_buffer.limiter()->is_full();
    }
    void set_buffer_finished() override {
        return _chunk_buffer.set_finished(_driver_sequence);
    }

    BalancedChunkBuffer& _chunk_buffer;
};


class DatacacheWarmupChunkSource final : public ChunkSource {
public:
    DatacacheWarmupChunkSource(ScanOperator* op, RuntimeProfile* runtime_profile, MorselPtr&& morsel, BalancedChunkBuffer& chunk_buffer)
            : ChunkSource(op, runtime_profile, std::move(morsel), chunk_buffer) {

    }

    ~DatacacheWarmupChunkSource() override = default;

    Status prepare(starrocks::RuntimeState *state) override {
        RETURN_IF_ERROR(ChunkSource::prepare(state));
        return Status::OK();
    }

    void close(starrocks::RuntimeState *state) override {
        std::cout << "warmup chunk source closed" << std::endl;
    }
private:
    Status _read_chunk(starrocks::RuntimeState *state, starrocks::ChunkPtr *chunk) override {
        std::cout << "end of file" << std::endl;
        ScanMorsel* scan_morsel = down_cast<ScanMorsel*>(_morsel.get());
        std::cout << "file path" << scan_morsel->get_scan_range()->hdfs_scan_range.relative_path << std::endl;

        ChunkPtr chunk_dst = std::make_shared<Chunk>();
        ColumnPtr column = ColumnHelper::create_column(TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR), true);
        column->append_default(10);
        chunk_dst->append_column(std::move(column), 0);
        *chunk = chunk_dst;
        return Status::EndOfFile("end of file");
    }

    const workgroup::WorkGroupScanSchedEntity * _scan_sched_entity(const workgroup::WorkGroup *wg) const override {
        DCHECK(wg != nullptr);
        return wg->scan_sched_entity();
    }
};


} // namespace starrocks::pipeline