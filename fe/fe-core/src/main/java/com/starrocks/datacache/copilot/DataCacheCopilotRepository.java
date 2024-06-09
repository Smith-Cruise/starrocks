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

package com.starrocks.datacache.copilot;

import com.google.common.collect.Sets;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.common.Config;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.privilege.PrivilegeBuiltinConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.statistic.StatisticExecutor;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.thrift.TStatisticData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Optional;

public class DataCacheCopilotRepository extends FrontendDaemon {
    private static final Logger LOG = LogManager.getLogger(DataCacheCopilotRepository.class);
    private long lastFlushTimestamp = 0L;
    private long lastDropTimestamp = 0L;


    public DataCacheCopilotRepository() {
        super("DataCacheCopilotRepository", 60 * 1000);
        lastFlushTimestamp = System.currentTimeMillis();
        lastDropTimestamp = System.currentTimeMillis();
    }

    @Override
    protected void runAfterCatalogReady() {
        tryToFlushToBE(false);
        tryToDropOutdatedStatistics();
    }

    private void tryToDropOutdatedStatistics() {
        if ((System.currentTimeMillis() - lastDropTimestamp) <= Config.datacache_copilot_delete_interval_secs * 1000) {
            return;
        }

        LOG.info("DataCacheCopilot repository start to drop outdated statistics");
        lastDropTimestamp = System.currentTimeMillis();

        try {
            String dropSQL = DataCacheCopilotSQLBuilder.buildDeleteOutdatedStatisticsSQL(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, StatsConstants.STATISTICS_DB_NAME,
                    DataCacheCopilotConstants.DATACACHE_COPILOT_STATISTICS_TABLE_NAME,
                    DataCacheCopilotConstants.ACCESS_TIME_COLUMN_NAME,
                    Config.datacache_copilot_statistics_keep_interval_secs);
            buildConnectContext().executeSql(dropSQL);
        } catch (Exception e) {
            LOG.warn("Failed to drop outdated data", e);
        }
    }

    private boolean isSatisfyFlushCondition() {
        if ((System.currentTimeMillis() - lastFlushTimestamp) > Config.datacache_copilot_flush_interval_secs * 1000) {
            return true;
        }

        if (FrequencyStatisticsStorage.getInstance().getEstimateMemorySize() >= Config.datacache_copilot_max_flight_bytes) {
            return true;
        }
        return false;
    }

    public void tryToFlushToBE(boolean isForce) {
        if (!isForce && !isSatisfyFlushCondition()) {
            return;
        }
        LOG.info("DataCacheCopilot repository flush data");

        lastFlushTimestamp = System.currentTimeMillis();

        Optional<String> insertSQL = FrequencyStatisticsStorage.getInstance()
                .exportInsertSQL(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, StatsConstants.STATISTICS_DB_NAME,
                        DataCacheCopilotConstants.DATACACHE_COPILOT_STATISTICS_TABLE_NAME);
        if (insertSQL.isEmpty()) {
            return;
        }
        try {
            buildConnectContext().executeSql(insertSQL.get());
        } catch (Exception e) {
            LOG.warn("Failed to flush data", e);
        }
    }

    public List<TStatisticData> collectCopilotStatistics(String sql) {
        StatisticExecutor executor = new StatisticExecutor();
        ConnectContext context = buildConnectContext();
        return executor.executeStatisticDQL(context, sql);
    }

    private ConnectContext buildConnectContext() {
        ConnectContext context = new ConnectContext();
        // Note: statistics query does not register query id to QeProcessorImpl::coordinatorMap,
        // but QeProcessorImpl::reportExecStatus will check query id,
        // So we must disable report query status from BE to FE
        context.getSessionVariable().setEnableProfile(false);
        // context.getSessionVariable().setQueryTimeoutS((int) Config.statistic_collect_query_timeout);
        // context.getSessionVariable().setEnablePipelineEngine(true);

        // context.setStatisticsContext(true);
        // context.setDatabase(StatsConstants.STATISTICS_DB_NAME);
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        context.setCurrentUserIdentity(UserIdentity.ROOT);
        context.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));
        context.setQualifiedUser(UserIdentity.ROOT.getUser());
        context.setQueryId(UUIDUtil.genUUID());
        // context.setExecutionId(UUIDUtil.toTUniqueId(context.getQueryId()));
        context.setStartTime();

        return context;
    }
}
