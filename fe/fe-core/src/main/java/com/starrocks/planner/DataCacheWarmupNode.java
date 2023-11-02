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
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.connector.RemoteFileBlockDesc;
import com.starrocks.connector.RemoteFileDesc;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.datacache.DataCacheOptions;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.thrift.TDataCacheOptions;
import com.starrocks.thrift.TDataCacheWarmupNode;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class DataCacheWarmupNode extends ScanNode {
    private final Table table;

    private DescriptorTable descTbl;

    public DataCacheWarmupNode(PlanNodeId id, TupleDescriptor tupleDescriptor, String planNodeName, Table table) {
        super(id, tupleDescriptor, planNodeName);
        this.table = table;
    }

    public void setupScanRangeLocations(DescriptorTable descTbl) {
        this.descTbl = descTbl;
    }

    public String getTableLocation() {
        return table.getTableLocation();
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.DATACACHE_WARMUP_NODE;
        msg.datacache_warmup_node = new TDataCacheWarmupNode();
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        List<PartitionKey> partitionKeys = Collections.singletonList(new PartitionKey());
        List<RemoteFileInfo> partitions = GlobalStateMgr.getCurrentState().getMetadataMgr().getRemoteFileInfos(table.getCatalogName(),
                table, partitionKeys);

        List<TScanRangeLocations> result = new LinkedList<>();

        if (table instanceof HiveTable) {
            for (RemoteFileInfo partition : partitions) {
                for (RemoteFileDesc fileDesc : partition.getFiles()) {
                    if (fileDesc.getLength() == 0) {
                        continue;
                    }
                    for (RemoteFileBlockDesc blockDesc : fileDesc.getBlockDescs()) {
                        result.add(createScanRangeLocations(partition, fileDesc, blockDesc, null));
                    }
                }
            }
        } else {
            throw new RuntimeException("Unsupported");
        }
        return result;
    }

    @Override
    public String getTableName() {
        return table.getName();
    }

    private TScanRangeLocations createScanRangeLocations(RemoteFileInfo partition,
                                                         RemoteFileDesc fileDesc, RemoteFileBlockDesc blockDesc,
                                                         DataCacheOptions dataCacheOptions) {
        TScanRangeLocations scanRangeLocations = new TScanRangeLocations();

        THdfsScanRange hdfsScanRange = new THdfsScanRange();
        hdfsScanRange.setFull_path(partition.getFullPath() + "/" + fileDesc.getFileName());
        hdfsScanRange.setOffset(blockDesc.getOffset());
        hdfsScanRange.setLength(blockDesc.getLength());
        hdfsScanRange.setFile_length(fileDesc.getLength());
        hdfsScanRange.setModification_time(fileDesc.getModificationTime());

        if (dataCacheOptions != null) {
            TDataCacheOptions tDataCacheOptions = new TDataCacheOptions();
            dataCacheOptions.toThrift(tDataCacheOptions);
            hdfsScanRange.setDatacache_options(tDataCacheOptions);
        }

        TScanRange scanRange = new TScanRange();
        scanRange.setHdfs_scan_range(hdfsScanRange);
        scanRangeLocations.setScan_range(scanRange);

        if (blockDesc.getReplicaHostIds().length == 0) {
            String message = String.format("hdfs file block has no host. file = %s/%s",
                    partition.getFullPath(), fileDesc.getFileName());
            throw new StarRocksPlannerException(message, ErrorType.INTERNAL_ERROR);
        }

        for (long hostId : blockDesc.getReplicaHostIds()) {
            String host = blockDesc.getDataNodeIp(hostId);
            TScanRangeLocation scanRangeLocation = new TScanRangeLocation(new TNetworkAddress(host, -1));
            scanRangeLocations.addToLocations(scanRangeLocation);
        }

        return scanRangeLocations;
    }
}
