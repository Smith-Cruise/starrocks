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
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.thrift.TResultSinkType;
import com.starrocks.thrift.TUniqueId;

public class WarmupJob {

    private DescriptorTable desc = new DescriptorTable();
    private TupleDescriptor exportTupleDesc = null;

    public WarmupJob() {
        exportTupleDesc = desc.createTupleDescriptor();
        SlotDescriptor slotDescriptor = desc.addSlotDescriptor(exportTupleDesc, new SlotId(0));
        slotDescriptor.setColumn(new Column("hello", ScalarType.createDefaultExternalTableString()));
        slotDescriptor.setIsNullable(true);
    }

    public Coordinator plan(TUniqueId id) {
        ScanNode scanNode = genScanNode();
        PlanFragment fragment = genPlanFragment(scanNode);
        return genCoordinators(id, fragment, scanNode);
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

    private Coordinator.Factory getCoordinatorFactory() {
        return new DefaultCoordinator.Factory();
    }

    private Coordinator genCoordinators(TUniqueId id, PlanFragment fragment, ScanNode scanNode) {
        return getCoordinatorFactory().createDatacacheWarmupScheduler(
                id, desc, Lists.newArrayList(fragment), Lists.newArrayList(scanNode),
                TimeUtils.DEFAULT_TIME_ZONE, System.currentTimeMillis());
    }
}
