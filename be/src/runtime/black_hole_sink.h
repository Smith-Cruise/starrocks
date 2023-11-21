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

#include "column/chunk.h"
#include "common/statusor.h"
#include "gen_cpp/InternalService_types.h"
#include "runtime/buffer_control_block.h"
#include "runtime/result_writer.h"
#include "runtime/runtime_state.h"

namespace starrocks {

class BlackHoleSink : public ResultWriter {
public:
    BlackHoleSink(BufferControlBlock* sinker, RuntimeProfile* parent_profile)
            : _sinker(sinker), _parent_profile(parent_profile) {}

    Status init(starrocks::RuntimeState* state) override { return Status::OK(); }

    Status append_chunk(starrocks::Chunk* chunk) override {
        return Status::NotSupported("Unsupported in none-pipeline engine");
    }

    StatusOr<TFetchDataResultPtrs> process_chunk(starrocks::Chunk* chunk) override {
        std::cout << "eat " << chunk->num_rows() << std::endl;
        TFetchDataResultPtrs results{};
        std::unique_ptr<TFetchDataResult> result(new (std::nothrow) TFetchDataResult());
        if (!result) {
            return Status::MemoryAllocFailed("memory allocate failed");
        }
        result->result_batch.rows.resize(0);
        results.emplace_back(std::move(result));
        return results;
    }

    StatusOr<bool> try_add_batch(starrocks::TFetchDataResultPtrs& results) override {
        size_t num_rows = 0;
        for (auto& result : results) {
            num_rows += result->result_batch.rows.size();
        }

        auto status = _sinker->try_add_batch(results);

        if (status.ok()) {
            if (status.value()) {
                _written_rows += num_rows;
                results.clear();
            }
        } else {
            results.clear();
            LOG(WARNING) << "Append statistic result to sink failed.";
        }
        return status;
    }

    Status close() override { return Status::OK(); }

private:
    BufferControlBlock* _sinker;
    RuntimeProfile* _parent_profile; // parent profile from result sink. not owned
};

} // namespace starrocks
