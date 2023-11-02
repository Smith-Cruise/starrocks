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
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.common.util.TimeUtils;
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
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.thrift.TResultSinkType;
import com.starrocks.thrift.TUniqueId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

public class DataCacheWarmupJob {
    private static final Logger LOG = LogManager.getLogger(DataCacheWarmupJob.class);

    private static final ShowResultSetMetaData METADATA = new ShowResultSetMetaData(ImmutableList.of(
            new Column("Status", ScalarType.createDefaultExternalTableString())));

    private final ConnectContext connectContext;
    private final String catalogName;
    private final String dbName;
    private final String tblName;

    private Table table;

    private DescriptorTable descTbl = new DescriptorTable();

    private TupleDescriptor tupleDescriptor = null;

    public DataCacheWarmupJob(ConnectContext connectContext, String catalogName, String dbName, String tblName) {
        this.connectContext = connectContext;
        this.catalogName = catalogName;
        this.dbName = dbName;
        this.tblName = tblName;
    }

    public void init() {
        MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();
        table = metadataMgr.getTable(catalogName, dbName, tblName);
        if (!table.isUnPartitioned()) {
            throw new RuntimeException("Don't support partitioned table now");
        }
        tupleDescriptor = descTbl.createTupleDescriptor();
        //        tupleDescriptor = descTbl.createTupleDescriptor();
        //        tupleDescriptor.setTable(table);
        //        tupleDescriptor.addSlot(
        //                new SlotDescriptor(new SlotId(0), "Status", ScalarType.createDefaultExternalTableString(), true));
    }

    public List<RowBatch> execute() {
        List<RowBatch> result = new LinkedList<>();
        UUID uuid = UUID.randomUUID();
        TUniqueId queryId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        ScanNode scanNode = genScanNode();
        PlanFragment planFragment = genPlanFragment(scanNode);
        Coordinator coordinator =
                new DefaultCoordinator.Factory().createDatacacheWarmupScheduler(queryId, connectContext, descTbl,
                Lists.newArrayList(planFragment), Lists.newArrayList(scanNode), TimeUtils.DEFAULT_TIME_ZONE,
                System.currentTimeMillis());

        try {
            QeProcessorImpl.INSTANCE.registerQuery(queryId, coordinator);
            coordinator.exec();
            {
                RowBatch batch = null;
                do {
                    batch = coordinator.getNext();
                    if (batch.getBatch() != null) {
                        result.add(batch);
                    }
                } while (!batch.isEos());
            }
            boolean res = coordinator.join(connectContext.getSessionVariable().getQueryTimeoutS());
            if (!res) {
                throw new RuntimeException("timeout");
            }
        } catch (Exception e) {
            LOG.warn(e);
            throw new RuntimeException("error");
        } finally {
            QeProcessorImpl.INSTANCE.unregisterQuery(queryId);
        }
        return result;
    }

    public ScanNode genScanNode() {
        // scanNode.setupScanRangeLocations(descTbl);
        return new DataCacheWarmupNode(new PlanNodeId(0), tupleDescriptor, "DataCacheWarmUpNode", table);
    }

    private PlanFragment genPlanFragment(ScanNode scanNode) {
        PlanFragment fragment = new PlanFragment(new PlanFragmentId(0), scanNode, DataPartition.UNPARTITIONED);

        // fragment.setOutputExprs(createOutputExprs());
        List<Expr> exprs = new LinkedList<>();
        SlotRef slotRef1 = new SlotRef(new SlotId(0));
        slotRef1.setType(ScalarType.createDefaultExternalTableString());
        SlotRef slotRef2 = new SlotRef(new SlotId(1));
        slotRef2.setType(ScalarType.createDefaultExternalTableString());
        exprs.add(slotRef1);
        exprs.add(slotRef2);
        fragment.setOutputExprs(exprs);
        scanNode.setFragmentId(fragment.getFragmentId());
        fragment.createDataSink(TResultSinkType.MYSQL_PROTOCAL);
        return fragment;
    }
}
