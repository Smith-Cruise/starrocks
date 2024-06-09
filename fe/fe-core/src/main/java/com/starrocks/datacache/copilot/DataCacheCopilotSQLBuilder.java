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

import com.starrocks.analysis.LimitElement;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.statistic.StatsConstants;

import java.util.ArrayList;
import java.util.List;

public class DataCacheCopilotSQLBuilder {

    private static final String DROP_TEMPLATE = "DELETE FROM %s.%s.%s WHERE %s <= from_unixtime(%d)";

    public static String buildDeleteOutdatedStatisticsSQL(String catalogName, String dbName, String tblName,
                                                          String accessTimeColumnName,
                                                          long interval) {
        return String.format(DROP_TEMPLATE, catalogName, dbName, tblName, accessTimeColumnName,
                System.currentTimeMillis() / 1000 - interval);
    }

    private static final String COLLECT_SQL_TEMPLATE =
            "SELECT CAST(%d AS INT), \n" +
                    "`" + DataCacheCopilotConstants.CATALOG_COLUMN_NAME + "`, \n" +
                    "`" + DataCacheCopilotConstants.DATABASE_COLUMN_NAME + "`, \n" +
                    "`" + DataCacheCopilotConstants.TABLE_COLUMN_NAME + "`, \n" +
                    "`" + DataCacheCopilotConstants.PARTITION_COLUMN_NAME + "`, \n" +
                    "`" + DataCacheCopilotConstants.COLUMN_COLUMN_NAME + "`, \n" +
                    "`" + DataCacheCopilotConstants.COUNT_COLUMN_NAME + "` \n" +
                    "FROM `%s`.`%s`.`%s`%s\n" +
                    "ORDER BY `" + DataCacheCopilotConstants.COUNT_COLUMN_NAME + "` DESC%s";

    public static String buildCollectStatisticsSQL(QualifiedName target, long interval, LimitElement limitElement) {
        String where = "";
        if (target != null || interval > 0) {
            where = buildWhereCondition(target, interval);
        }

        String limit = "";
        if (limitElement != null) {
            limit = buildLimitOffset(limitElement);
        }

        return String.format(
                COLLECT_SQL_TEMPLATE, StatsConstants.STATISTICS_DATACACHE_COPILOT_VERSION,
                InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                StatsConstants.STATISTICS_DB_NAME,
                DataCacheCopilotConstants.DATACACHE_COPILOT_STATISTICS_TABLE_NAME,
                where,
                limit).trim();
    }

    private static String buildWhereCondition(QualifiedName qualifiedName, long interval) {
        List<String> conditions = new ArrayList<>();

        if (interval > 0) {
            conditions.add(
                    String.format("`" + DataCacheCopilotConstants.ACCESS_TIME_COLUMN_NAME + "` >= from_unixtime(%d)",
                            System.currentTimeMillis() / 1000 - interval));
        }

        if (qualifiedName != null) {
            List<String> parts = qualifiedName.getParts();
            for (int i = 0; i < parts.size(); i++) {
                if (i == 0) {
                    conditions.add(String.format("`" + DataCacheCopilotConstants.CATALOG_COLUMN_NAME + "` = '%s'",
                            parts.get(0)));
                } else if (i == 1) {
                    conditions.add(String.format("`" + DataCacheCopilotConstants.DATABASE_COLUMN_NAME + "` = '%s'",
                            parts.get(1)));
                } else if (i == 2) {
                    conditions.add(String.format("`" + DataCacheCopilotConstants.COLUMN_COLUMN_NAME + "` = '%s'",
                            parts.get(2)));
                } else {
                    break;
                }
            }
        }

        StringBuilder sb = new StringBuilder();
        if (!conditions.isEmpty()) {
            sb.append(" WHERE ");
            sb.append(String.join(" AND ", conditions));
        }
        return sb.toString();
    }

    private static String buildLimitOffset(LimitElement limitElement) {
        return limitElement.toSql();
    }
}
