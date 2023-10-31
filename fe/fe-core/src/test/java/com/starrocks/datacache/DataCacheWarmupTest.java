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

import com.starrocks.planner.ScanNode;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TScanRangeLocations;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class DataCacheWarmupTest extends PlanTestBase {
    private final DataCacheMgr dataCacheMgr = DataCacheMgr.getInstance();

    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        AnalyzeTestUtil.setConnectContext(connectContext);
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);
    }

    @Test
    public void testScanRange() {
        DataCacheWarmupTask task = new DataCacheWarmupTask("hive0", "datacache_db", "normal_table");
        ScanNode scanNode = task.genScanNode();
        List<TScanRangeLocations> scanRanges = scanNode.getScanRangeLocations(0);
        for (TScanRangeLocations location : scanRanges) {
            System.out.println(location.scan_range.hdfs_scan_range.full_path);
        }
    }
}
