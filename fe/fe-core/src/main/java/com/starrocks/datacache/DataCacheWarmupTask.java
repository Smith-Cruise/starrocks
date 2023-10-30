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
import com.starrocks.common.util.TimeUtils;
import com.starrocks.planner.DataCacheWarmupNode;
import com.starrocks.planner.DataPartition;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.ScanNode;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.QeProcessorImpl;
import com.starrocks.qe.RowBatch;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.thrift.TResultSinkType;
import com.starrocks.thrift.TUniqueId;

import java.util.UUID;

public class DataCacheWarmupTask {

    private DescriptorTable desc = new DescriptorTable();
    private TupleDescriptor exportTupleDesc = null;

    public DataCacheWarmupTask() {
        exportTupleDesc = desc.createTupleDescriptor();
        SlotDescriptor slotDescriptor = desc.addSlotDescriptor(exportTupleDesc, new SlotId(0));
        slotDescriptor.setColumn(new Column("hello", ScalarType.createDefaultExternalTableString()));
        slotDescriptor.setIsNullable(true);
    }

    public ShowResultSet execute() {
        UUID uuid = UUID.randomUUID();
        TUniqueId queryId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        System.out.println(queryId);
        ScanNode scanNode = genScanNode();
        PlanFragment planFragment = genPlanFragment(scanNode);
        Coordinator coordinator = new DefaultCoordinator.Factory().createDatacacheWarmupScheduler(queryId, desc,
                Lists.newArrayList(planFragment), Lists.newArrayList(scanNode), TimeUtils.DEFAULT_TIME_ZONE,
                System.currentTimeMillis());
        try {
            QeProcessorImpl.INSTANCE.registerQuery(queryId, coordinator);
            coordinator.exec();
            RowBatch rowBatch = coordinator.getNext();
            System.out.println(rowBatch.getBatch().toString());
            boolean res = coordinator.join(5);
            if (res) {
                System.out.println("success");
            } else {
                System.out.println("timeout");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            QeProcessorImpl.INSTANCE.unregisterQuery(queryId);
        }
        ShowResultSetMetaData metaData = new ShowResultSetMetaData(ImmutableList.of(new Column("name",
                ScalarType.createDefaultExternalTableString())));
        return new ShowResultSet(metaData, ImmutableList.of(ImmutableList.of("hello")));
    }

    private ScanNode genScanNode() {
        return new DataCacheWarmupNode(new PlanNodeId(0), exportTupleDesc, "WarmUpNode");
    }

    private PlanFragment genPlanFragment(ScanNode scanNode) {
        PlanFragment fragment = new PlanFragment(new PlanFragmentId(0), scanNode, DataPartition.UNPARTITIONED);

        // fragment.setOutputExprs(createOutputExprs());
        scanNode.setFragmentId(fragment.getFragmentId());
        fragment.createDataSink(TResultSinkType.MYSQL_PROTOCAL);
        return fragment;
    }
}
