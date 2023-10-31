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

package com.starrocks.datacache;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.mysql.MysqlChannel;
import com.starrocks.planner.DataCacheWarmupNode;
import com.starrocks.planner.DataPartition;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.ScanNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.QeProcessorImpl;
import com.starrocks.qe.RowBatch;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.thrift.TResultSinkType;
import com.starrocks.thrift.TUniqueId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.UUID;

public class DataCacheWarmupTask {
    private static final Logger LOG = LogManager.getLogger(DataCacheWarmupTask.class);

    private static final ShowResultSetMetaData METADATA = new ShowResultSetMetaData(ImmutableList.of(
            new Column("Status", ScalarType.createDefaultExternalTableString())));

    private final Table table;

    private DescriptorTable descTbl = new DescriptorTable();

    private TupleDescriptor tupleDescriptor = null;

    public DataCacheWarmupTask(String catalogName, String dbName, String tblName) {
        MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();
        table = metadataMgr.getTable(catalogName, dbName, tblName);

        tupleDescriptor = descTbl.createTupleDescriptor();
        tupleDescriptor.setTable(table);
        tupleDescriptor.addSlot(
                new SlotDescriptor(new SlotId(0), "Status", ScalarType.createDefaultExternalTableString(), true));
    }

    public ShowResultSet execute() {
        UUID uuid = UUID.randomUUID();
        TUniqueId queryId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        System.out.println(queryId);
        ScanNode scanNode = genScanNode();
        PlanFragment planFragment = genPlanFragment(scanNode);
        Coordinator coordinator = new DefaultCoordinator.Factory().createDatacacheWarmupScheduler(queryId, descTbl,
                Lists.newArrayList(planFragment), Lists.newArrayList(scanNode), TimeUtils.DEFAULT_TIME_ZONE,
                System.currentTimeMillis());

        ShowResultSet showResultSet;
        try {
            QeProcessorImpl.INSTANCE.registerQuery(queryId, coordinator);
            coordinator.exec();

            ConnectContext context = ConnectContext.get();
            {
                MysqlChannel channel = context.getMysqlChannel();
                RowBatch batch = null;
                boolean isSendFields = false;
                do {
                    batch = coordinator.getNext();
                    //                    // for outfile query, there will be only one empty batch send back with eos flag
                    //                    if (batch.getBatch() != null) {
                    //                        // For some language driver, getting error packet after fields packet will be recognized as a success result
                    //                        // so We need to send fields after first batch arrived
                    //                        if (!isSendFields) {
                    //                            //                            sendFields(colNames, outputExprs);
                    //                            isSendFields = true;
                    //                        }
                    //                        if (channel.isSendBufferNull()) {
                    //                            int bufferSize = 0;
                    //                            for (ByteBuffer row : batch.getBatch().getRows()) {
                    //                                bufferSize += (row.position() - row.limit());
                    //                            }
                    //                            // +8 for header size
                    //                            channel.initBuffer(bufferSize + 8);
                    //                        }
                    //
                    //                        for (ByteBuffer row : batch.getBatch().getRows()) {
                    //                            channel.sendOnePacket(row);
                    //                        }
                    //                        context.updateReturnRows(batch.getBatch().getRows().size());
                    //                    }
                } while (!batch.isEos());
                if (!isSendFields) {
                    //                    sendFields(colNames, outputExprs);
                }
            }
            boolean res = coordinator.join(300);
            if (res) {
                showResultSet = new ShowResultSet(METADATA, ImmutableList.of(ImmutableList.of("success")));
            } else {
                showResultSet = new ShowResultSet(METADATA, ImmutableList.of(ImmutableList.of("execute timeout")));
            }
        } catch (Exception e) {
            LOG.warn(e);
            showResultSet = new ShowResultSet(METADATA, ImmutableList.of(ImmutableList.of("error")));
        } finally {
            QeProcessorImpl.INSTANCE.unregisterQuery(queryId);
        }

        return showResultSet;
    }

    public ScanNode genScanNode() {
        DataCacheWarmupNode scanNode = new DataCacheWarmupNode(new PlanNodeId(0), tupleDescriptor, "WarmUpNode", table);
        scanNode.setupScanRangeLocations(descTbl);
        return scanNode;
    }

    private PlanFragment genPlanFragment(ScanNode scanNode) {
        PlanFragment fragment = new PlanFragment(new PlanFragmentId(0), scanNode, DataPartition.UNPARTITIONED);

        // fragment.setOutputExprs(createOutputExprs());
        scanNode.setFragmentId(fragment.getFragmentId());
        fragment.createDataSink(TResultSinkType.MYSQL_PROTOCAL);
        return fragment;
    }
}
