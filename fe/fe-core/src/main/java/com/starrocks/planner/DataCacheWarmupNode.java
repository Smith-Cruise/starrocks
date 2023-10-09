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

package com.starrocks.planner;

import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.thrift.TDataCacheWarmupNode;
import com.starrocks.thrift.THdfsFileFormat;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocations;

import java.util.ArrayList;
import java.util.List;

public class DataCacheWarmupNode extends ScanNode {

    public DataCacheWarmupNode(PlanNodeId id, TupleDescriptor tupleDescriptor, String planNodeName) {
        super(id, tupleDescriptor, planNodeName);
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.DATACACHE_WARMUP_NODE;
        TDataCacheWarmupNode node = new TDataCacheWarmupNode();
        List<String> list = new ArrayList<>();
        list.add("hello");
        list.add("world");
        node.setScan_ranges(list);
        msg.datacache_warmup_node = node;
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        List<TScanRangeLocations> tScanRangeLocationsList = new ArrayList<>();
        TScanRangeLocations locations = new TScanRangeLocations();
        THdfsScanRange hdfsScanRange = new THdfsScanRange();
        hdfsScanRange.setRelative_path("filename");
        hdfsScanRange.setOffset(0);
        hdfsScanRange.setLength(1024);
        hdfsScanRange.setPartition_id(0);
        hdfsScanRange.setFile_length(1024);
        hdfsScanRange.setModification_time(System.currentTimeMillis());
        hdfsScanRange.setFile_format(THdfsFileFormat.PARQUET);

        TScanRange scanRange = new TScanRange();
        scanRange.setHdfs_scan_range(hdfsScanRange);
        locations.setScan_range(scanRange);

        tScanRangeLocationsList.add(locations);
        return tScanRangeLocationsList;
    }
}
