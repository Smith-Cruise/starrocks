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

import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.common.OptimizerSetting;
import com.starrocks.system.Backend;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public class DataCacheDetector {
    private final String warmupSQL;
    private final String currentCatalog;
    private final String currentDatabase;

    public DataCacheDetector(String sql, String catalog, String database) {
        this.warmupSQL = sql;
        this.currentCatalog = catalog;
        this.currentDatabase = database;
    }

    private ConnectContext createConnectContext() {
        ConnectContext connectContext = new ConnectContext(null);
        connectContext.setCurrentCatalog(currentCatalog);
        connectContext.setDatabase(currentDatabase);
        connectContext.setQualifiedUser(AuthenticationMgr.ROOT_USER);
        connectContext.setCurrentUserIdentity(
                UserIdentity.createAnalyzedUserIdentWithIp(AuthenticationMgr.ROOT_USER, "%"));
        connectContext.setCurrentRoleIds(connectContext.getCurrentUserIdentity());
        UUID uuid = UUID.randomUUID();
        connectContext.setQueryId(uuid);
        connectContext.getSessionVariable().setEnableAsyncProfile(false);
        connectContext.getSessionVariable().setEnableProfile(true);
        connectContext.getSessionVariable().setEnablePopulateDatacache(false);
        connectContext.getSessionVariable().setEnableScanDataCache(true);
        connectContext.getSessionVariable().setEnableWarmupDataCache(true);
        return connectContext;
    }

    public void checkCanCacheSafely() throws Exception {
        double cacheHitRatio = getSampleDataCacheHitRatio();

        Map<Long, Long> mapping = collectBackendScanRangeSize();

        boolean isFailed = false;
        StringBuilder errorMsg = new StringBuilder();
        for (Map.Entry<Long, Long> entry : mapping.entrySet()) {
            long backendId = entry.getKey();
            long totalBytes = (long) Math.ceil(entry.getValue() * cacheHitRatio);
            Backend backend = GlobalStateMgr.getCurrentSystemInfo().getBackend(backendId);
            Optional<DataCacheMetrics> dataCacheMetrics = backend.getDataCacheMetrics();
            if (dataCacheMetrics.isEmpty()) {
                isFailed = true;
                errorMsg.append(String.format("BackendId: %s didn't collect metrics. ", backendId));
                continue;
            }

            long totalDataCacheSize =
                    dataCacheMetrics.get().getDiskQuotaBytes() + dataCacheMetrics.get().getMemQuoteBytes();
            if (totalBytes > totalDataCacheSize) {
                isFailed = true;
                errorMsg.append(String.format(
                        "BackendId: %s didn't have enough datacache quota. Total quota: %s. Required datacache size:%s. ",
                        backendId, totalDataCacheSize, totalBytes));
            }
        }

        if (isFailed) {
            throw new Exception(errorMsg.toString());
        }
    }

    private Map<Long, Long> collectBackendScanRangeSize() throws Exception {
        ConnectContext connectContext = createConnectContext();
        OptimizerSetting optimizerSetting = OptimizerSetting.builder()
                .setEnableCollectDataCacheSize(true).build();
        connectContext.setOptimizerSetting(optimizerSetting);

        String sampleSQL = String.format("INSERT INTO blackhole() %s", warmupSQL);
        StmtExecutor stmtExecutor = connectContext.executeSql(sampleSQL);

        if (!stmtExecutor.getCoordinator().isDone()) {
            throw new Exception("failed");
        }

        return DataCacheDetectRecorder.getBackendToScanRangeSize();
    }

    private double getSampleDataCacheHitRatio() throws Exception {
        ConnectContext connectContext = createConnectContext();
        OptimizerSetting optimizerSetting = OptimizerSetting.builder()
                .setEnableDataCacheSample(true).build();
        connectContext.setOptimizerSetting(optimizerSetting);

        String sampleSQL = String.format("INSERT INTO blackhole() %s", warmupSQL);
        StmtExecutor stmtExecutor = connectContext.executeSql(sampleSQL);

        if (!stmtExecutor.getCoordinator().isDone()) {
            throw new Exception("failed");
        }

        DataCacheWarmupMetrics dataCacheWarmupMetrics = stmtExecutor.getCoordinator().getDataCacheWarmupBytes();
        long totalSize = DataCacheDetectRecorder.getTotalSize();
        return Math.max((double) dataCacheWarmupMetrics.getDataCacheWarmupNeedBytes() / (double) totalSize, 1);
    }
}
