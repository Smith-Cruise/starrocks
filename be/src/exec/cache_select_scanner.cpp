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

#include "exec/cache_select_scanner.h"
#include "fs/fs.h"
#include "io/shared_buffered_input_stream.h"
#include "config.h"
namespace starrocks {

Status CacheSelectScanner::do_init(RuntimeState* runtime_state, const HdfsScannerParams& scanner_params) {
    return Status::OK();
}

Status CacheSelectScanner::do_open(RuntimeState* runtime_state) {
    RETURN_IF_ERROR(open_random_access_file());
    return Status::OK();
}

void CacheSelectScanner::do_close(RuntimeState* runtime_state) noexcept {}

Status CacheSelectScanner::open_random_access_file() {
    CHECK(_file == nullptr) << "File has already been opened";
    ASSIGN_OR_RETURN(std::unique_ptr<RandomAccessFile> raw_file,
                     _scanner_params.fs->new_random_access_file(_scanner_params.path))
    const int64_t file_size = _scanner_params.file_size;
    raw_file->set_size(file_size);
    const std::string& filename = raw_file->filename();

    std::shared_ptr<io::SeekableInputStream> input_stream = raw_file->stream();

    input_stream = std::make_shared<CountedSeekableInputStream>(input_stream, &_fs_stats);

    _shared_buffered_input_stream = std::make_shared<io::SharedBufferedInputStream>(input_stream, filename, file_size);
    const io::SharedBufferedInputStream::CoalesceOptions options = {
            .max_dist_size = config::io_coalesce_read_max_distance_size,
            .max_buffer_size = config::io_coalesce_read_max_buffer_size};
    _shared_buffered_input_stream->set_coalesce_options(options);
    input_stream = _shared_buffered_input_stream;

    // input_stream = CacheInputStream(input_stream)
    if (_scanner_params.use_datacache) {
        _cache_input_stream = std::make_shared<io::CacheInputStream>(_shared_buffered_input_stream, filename, file_size,
                                                                     _scanner_params.modification_time);
        _cache_input_stream->set_enable_populate_cache(_scanner_params.enable_populate_datacache);
        _cache_input_stream->set_enable_async_populate_mode(_scanner_params.enable_datacache_async_populate_mode);
        _cache_input_stream->set_enable_cache_io_adaptor(_scanner_params.enable_datacache_io_adaptor);
        _cache_input_stream->set_priority(_scanner_params.datacache_priority);
        _cache_input_stream->set_ttl_seconds(_scanner_params.datacache_ttl_seconds);
        _cache_input_stream->set_enable_block_buffer(config::datacache_block_buffer_enable);
        _shared_buffered_input_stream->set_align_size(_cache_input_stream->get_align_size());
        input_stream = _cache_input_stream;
    }

    // input_stream = CountedInputStream(input_stream)
    // NOTE: make sure `CountedInputStream` is last applied, so io time can be accurately timed.
    input_stream = std::make_shared<CountedSeekableInputStream>(input_stream, &_app_stats);

    // so wrap function is f(x) = (CountedInputStream (CacheInputStream (DecompressInputStream (CountedInputStream x))))
    _file = std::make_unique<RandomAccessFile>(input_stream, filename);
    _file->set_size(file_size);
    return Status::OK();
}

Status CacheSelectScanner::do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) {
    if (_scanner_params.scan_range->file_format == THdfsFileFormat::TEXT) {
        RETURN_IF_ERROR(_fetch_textfile());
    }
    return Status::EndOfFile("");
}

Status CacheSelectScanner::_fetch_textfile() {
    // If it's compressed file, we only handle scan range whose offset == 0.
    if (_compression_type != NO_COMPRESSION && _scanner_params.scan_range->offset != 0) {
        return Status::OK();
    }

    const int64_t start_offset = _scanner_params.scan_range->offset;
    const int64_t end_offset = _scanner_params.scan_range->offset + _scanner_params.scan_range->length;

    if (_shared_buffered_input_stream != nullptr) {
        std::vector<io::SharedBufferedInputStream::IORange> ranges{};
        for (int64_t offset = start_offset; offset < end_offset;) {
            const int64_t remain_length = std::min(static_cast<int64_t>(config::io_coalesce_read_max_buffer_size), end_offset - offset);
            ranges.emplace_back(offset, remain_length);
            offset += remain_length;
        }
        RETURN_IF_ERROR(_shared_buffered_input_stream->set_io_ranges(ranges));
    }

    std::vector<char> dummy_buffer{};
    dummy_buffer.reserve(config::io_coalesce_read_max_buffer_size);
    for (int64_t offset = start_offset; offset < end_offset;) {
        const int64_t remain_length = std::min(static_cast<int64_t>(config::io_coalesce_read_max_buffer_size), end_offset - offset);
        RETURN_IF_ERROR(_file->read_at_fully(offset, dummy_buffer.data(), remain_length));
        offset += remain_length;
    }

    return Status::OK();
}


} // namespace starrocks
