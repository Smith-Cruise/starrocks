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

import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.connector.RemoteScanRangeLocations;
import com.starrocks.sql.plan.HDFSScanNodePredicates;
import com.starrocks.thrift.TDataCacheWarmupNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocations;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataCacheWarmupNode extends ScanNode {

    private final RemoteScanRangeLocations scanRangeLocations = new RemoteScanRangeLocations();
    private final HDFSScanNodePredicates scanNodePredicates = new HDFSScanNodePredicates();
    private final Table hiveTable;

    private DescriptorTable descTbl;

    public DataCacheWarmupNode(PlanNodeId id, TupleDescriptor tupleDescriptor, String planNodeName, Table hiveTable) {
        super(id, tupleDescriptor, planNodeName);
        this.hiveTable = hiveTable;


        Map<Long, PartitionKey> maps = new HashMap<>();
        maps.put(0L, new PartitionKey());
        scanNodePredicates.setIdToPartitionKey(maps);
        scanNodePredicates.setSelectedPartitionIds(Collections.singletonList(0L));
    }

    public void setupScanRangeLocations(DescriptorTable descTbl) {
        this.descTbl = descTbl;
        scanRangeLocations.setup(descTbl, hiveTable, scanNodePredicates);
    }

    public HDFSScanNodePredicates getScanNodePredicates() {
        return this.scanNodePredicates;
    }

    public String getTableLocation() {
        return "tablelocation";
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
        return scanRangeLocations.getScanRangeLocations(descTbl, hiveTable, scanNodePredicates);
    }

    @Override
    public String getTableName() {
        return "datacache table name";
    }
}
