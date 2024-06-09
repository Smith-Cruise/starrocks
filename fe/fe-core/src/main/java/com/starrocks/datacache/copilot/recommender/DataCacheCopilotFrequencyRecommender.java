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

package com.starrocks.datacache.copilot.recommender;

import com.google.common.collect.ImmutableSet;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.datacache.copilot.DataCacheCopilotRepository;
import com.starrocks.datacache.copilot.DataCacheCopilotSQLBuilder;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.RecommendDataCacheSelectStmt;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.thrift.TStatisticData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

public class DataCacheCopilotFrequencyRecommender implements DataCacheCopilotRecommender {

    private static final Logger LOG = LogManager.getLogger(DataCacheCopilotFrequencyRecommender.class);

    private final RecommendDataCacheSelectStmt stmt;

    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("RECOMMEND_SQL", ScalarType.createVarchar(500)))
                    .addColumn(new Column("WEIGHT", ScalarType.createVarcharType(20)))
                    .build();

    public DataCacheCopilotFrequencyRecommender(RecommendDataCacheSelectStmt stmt) {
        this.stmt = stmt;
    }

    @Override
    public ShowResultSet recommend() {
        // flush to be first
        DataCacheCopilotRepository repository = GlobalStateMgr.getCurrentState().getDataCacheCopilotRepository();
        repository.tryToFlushToBE(true);

        String sql = DataCacheCopilotSQLBuilder.buildCollectStatisticsSQL(stmt.getTarget(), stmt.getInterval(),
                stmt.getLimitElement());

        // get TStatisticData results from BE
        List<TStatisticData> tStatisticDataList = repository.collectCopilotStatistics(sql);
        // convert TStatisticData -> StatisticsData, so that we can apply some merge algorithm
        List<StatisticsData> statisticDataList = StatisticsData.buildFromThrift(tStatisticDataList);
        // merge adjacent StatisticsData
        List<StatisticsData> mergedStatisticsDataList = StatisticsData.mergeIsomorphism(statisticDataList);
        // generate cache select sql based on merged StatisticsData
        return buildCacheSelectSQLs(mergedStatisticsDataList);
    }

    private ShowResultSet buildCacheSelectSQLs(List<StatisticsData> rows) {
        List<List<String>> sqls = new LinkedList<>();

        for (StatisticsData row : rows) {
            Optional<String> buildSQL = CacheSelectSQLBuilder.build(row);
            if (buildSQL.isEmpty()) {
                // build cache select failed, ignore this row
                continue;
            }

            List<String> sql = new ArrayList<>(2);
            sql.add(buildSQL.get());
            sql.add(String.valueOf(row.count));
            sqls.add(sql);
        }

        return new ShowResultSet(META_DATA, sqls);
    }

    private static class StatisticsData {
        private final String catalogName;
        private final String databaseName;
        private final String tableName;
        private final ImmutableSet<String> partitionNames;
        private final ImmutableSet<String> columnNames;
        private final long count;

        private StatisticsData(String catalogName, String databaseName, String tableName,
                               ImmutableSet<String> partitionNames,
                               ImmutableSet<String> columnNames, long count) {
            this.catalogName = catalogName;
            this.databaseName = databaseName;
            this.tableName = tableName;
            this.partitionNames = partitionNames;
            this.columnNames = columnNames;
            this.count = count;
        }

        public static List<StatisticsData> buildFromThrift(List<TStatisticData> tStatisticDataList) {
            List<StatisticsData> statisticsDataList = new ArrayList<>(tStatisticDataList.size());
            for (TStatisticData tStatisticData : tStatisticDataList) {
                ImmutableSet.Builder<String> partitionNames = ImmutableSet.builder();
                if (!tStatisticData.getPartitionName().isBlank()) {
                    // partitionName empty means there is no predicate on partition column
                    partitionNames.add(tStatisticData.getPartitionName());
                }

                ImmutableSet.Builder<String> columnNames = ImmutableSet.builder();
                columnNames.add(tStatisticData.getColumnName());

                statisticsDataList.add(
                        new StatisticsData(tStatisticData.getCatalogName(), tStatisticData.getDatabaseName(),
                                tStatisticData.getTableName(), partitionNames.build(), columnNames.build(),
                        tStatisticData.getRowCount()));
            }
            return statisticsDataList;
        }

        // If catalogName, databaseName, tableName and count are the same, we will merge it
        public static List<StatisticsData> mergeIsomorphism(List<StatisticsData> tStatisticDataList) {
            List<StatisticsData> merged = new ArrayList<>();
            for (int left = 0; left < tStatisticDataList.size(); left++) {
                StatisticsData leftData = tStatisticDataList.get(left);
                final long count = leftData.count;
                final String catalogName = leftData.catalogName;
                final String databaseName = leftData.databaseName;
                final String tableName = leftData.tableName;

                ImmutableSet.Builder<String> partitionNames = ImmutableSet.builder();
                partitionNames.addAll(leftData.partitionNames);
                ImmutableSet.Builder<String> columnNames = ImmutableSet.builder();
                columnNames.addAll(leftData.columnNames);

                for (int right = left + 1; right < tStatisticDataList.size(); right++) {
                    StatisticsData rightData = tStatisticDataList.get(right);
                    if (!catalogName.equals(rightData.catalogName) || !databaseName.equals(rightData.databaseName) ||
                            !tableName.equals(rightData.tableName) || count != rightData.count) {
                        break;
                    }
                    partitionNames.addAll(rightData.partitionNames);
                    columnNames.addAll(rightData.columnNames);
                }

                merged.add(new StatisticsData(catalogName, databaseName, tableName, partitionNames.build(),
                        columnNames.build(), count));
            }
            return merged;
        }
    }

    private static class CacheSelectSQLBuilder {
        private static final String CACHE_SELECT_TEMPLATE = "CACHE SELECT %s FROM `%s`.`%s`.`%s` %s;";

        private static Optional<String> build(StatisticsData statisticsData) {
            try {
                Table table = MetaUtils.getTable(statisticsData.catalogName, statisticsData.databaseName,
                        statisticsData.tableName);
                String where = "";
                if (!statisticsData.partitionNames.isEmpty()) {
                    where = buildWhereCondition(table, statisticsData.partitionNames);
                }
                return Optional.of(String.format(CACHE_SELECT_TEMPLATE, buildColumn(statisticsData.columnNames),
                        statisticsData.catalogName, statisticsData.databaseName, statisticsData.tableName, where));
            } catch (Exception e) {
                LOG.warn("Failed to build cache select sql", e);
                return Optional.empty();
            }
        }

        private static String buildColumn(ImmutableSet<String> columns) {
            List<String> list = new ArrayList<>(columns.size());
            for (String column : columns) {
                list.add(String.format("`%s`", column));
            }
            return String.join(", ", list);
        }

        private static String buildWhereCondition(Table table, ImmutableSet<String> partitionNames)
                throws AnalysisException {
            List<Column> partitionColumns = PartitionUtil.getPartitionColumns(table);
            List<String> orPredicates = new ArrayList<>(partitionNames.size());
            for (String partitionName : partitionNames) {
                List<String> partitionValues = PartitionUtil.toPartitionValues(partitionName);
                PartitionKey partitionKey = PartitionUtil.createPartitionKey(partitionValues, partitionColumns, table);
                List<LiteralExpr> keys = partitionKey.getKeys();
                List<String> predicate = new ArrayList<>(keys.size());
                for (int i = 0; i < keys.size(); i++) {
                    predicate.add(String.format("`%s` = %s", partitionColumns.get(i).getName(),
                            keys.get(i).getStringValue()));
                }
                orPredicates.add(String.join(" AND ", predicate));
            }
            return "WHERE " + String.join(" OR ", orPredicates);
        }
    }
}
