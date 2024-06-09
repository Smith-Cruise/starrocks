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
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.RecommendDataCacheSelectStmt;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class DataCacheCopilotFrequencyRecommenderTest {
    @Test
    public void testBuildSQL() {
        RecommendDataCacheSelectStmt stmt = new RecommendDataCacheSelectStmt(null, 0, null, null);
        String sql = DataCacheCopilotSQLBuilder.buildCollectStatisticsSQL(stmt.getTarget(), stmt.getInterval(),
                stmt.getLimitElement());
        Assert.assertEquals("SELECT CAST(20 AS INT), \n" +
                "`catalog_name`, \n" +
                "`database_name`, \n" +
                "`table_name`, \n" +
                "`partition_name`, \n" +
                "`column_name`, \n" +
                "`count` \n" +
                "FROM `default_catalog`.`_statistics_`.`datacache_copilot_statistics`\n" +
                "ORDER BY `count` DESC", sql);
    }

    @Test
    public void testBuildSQLWithQualifiedName() {
        RecommendDataCacheSelectStmt stmt =
                new RecommendDataCacheSelectStmt(null, 0, null, null);
        String sql = DataCacheCopilotSQLBuilder.buildCollectStatisticsSQL(stmt.getTarget(), stmt.getInterval(),
                stmt.getLimitElement());
        Assert.assertEquals("SELECT CAST(20 AS INT), \n" +
                "`catalog_name`, \n" +
                "`database_name`, \n" +
                "`table_name`, \n" +
                "`partition_name`, \n" +
                "`column_name`, \n" +
                "`count` \n" +
                "FROM `default_catalog`.`_statistics_`.`datacache_copilot_statistics`\n" +
                "ORDER BY `count` DESC", sql);

        stmt = new RecommendDataCacheSelectStmt(QualifiedName.of(List.of("catalog"), null), 0, null, null);
        sql = DataCacheCopilotSQLBuilder.buildCollectStatisticsSQL(stmt.getTarget(), stmt.getInterval(),
                stmt.getLimitElement());
        Assert.assertEquals("SELECT CAST(20 AS INT), \n" +
                "`catalog_name`, \n" +
                "`database_name`, \n" +
                "`table_name`, \n" +
                "`partition_name`, \n" +
                "`column_name`, \n" +
                "`count` \n" +
                "FROM `default_catalog`.`_statistics_`.`datacache_copilot_statistics` WHERE `catalog_name` = 'catalog'\n" +
                "ORDER BY `count` DESC", sql);

        stmt = new RecommendDataCacheSelectStmt(QualifiedName.of(List.of("catalog", "db"), null), 0, null, null);
        sql = DataCacheCopilotSQLBuilder.buildCollectStatisticsSQL(stmt.getTarget(), stmt.getInterval(),
                stmt.getLimitElement());
        Assert.assertEquals("SELECT CAST(20 AS INT), \n" +
                "`catalog_name`, \n" +
                "`database_name`, \n" +
                "`table_name`, \n" +
                "`partition_name`, \n" +
                "`column_name`, \n" +
                "`count` \n" +
                "FROM `default_catalog`.`_statistics_`.`datacache_copilot_statistics` " +
                "WHERE `catalog_name` = 'catalog' AND `database_name` = 'db'\n" +
                "ORDER BY `count` DESC", sql);

        stmt = new RecommendDataCacheSelectStmt(QualifiedName.of(List.of("catalog", "db", "tbl"), null), 0, null, null);
        sql = DataCacheCopilotSQLBuilder.buildCollectStatisticsSQL(stmt.getTarget(), stmt.getInterval(),
                stmt.getLimitElement());
        Assert.assertEquals("SELECT CAST(20 AS INT), \n" +
                "`catalog_name`, \n" +
                "`database_name`, \n" +
                "`table_name`, \n" +
                "`partition_name`, \n" +
                "`column_name`, \n" +
                "`count` \n" +
                "FROM `default_catalog`.`_statistics_`.`datacache_copilot_statistics` " +
                "WHERE `catalog_name` = 'catalog' AND `database_name` = 'db' AND `table_name` = 'tbl'\n" +
                "ORDER BY `count` DESC", sql);
    }

    @Test
    public void testBuildSQLWithInterval() {
        RecommendDataCacheSelectStmt stmt =
                new RecommendDataCacheSelectStmt(null, 24 * 3600, null, null);
        String sql = DataCacheCopilotSQLBuilder.buildCollectStatisticsSQL(stmt.getTarget(), stmt.getInterval(),
                stmt.getLimitElement());
        Assert.assertTrue(sql.contains("WHERE `access_time` >= from_unixtime"));

        stmt = new RecommendDataCacheSelectStmt(QualifiedName.of(List.of("catalog")), 24 * 3600, null, null);
        sql = DataCacheCopilotSQLBuilder.buildCollectStatisticsSQL(stmt.getTarget(), stmt.getInterval(),
                stmt.getLimitElement());
        Assert.assertTrue(sql.contains("WHERE `access_time` >= from_unixtime"));
        Assert.assertTrue(sql.contains("AND `catalog_name` = 'catalog'"));
    }

    @Test
    public void testBuildSQLWithLimitElement() {
        RecommendDataCacheSelectStmt stmt = new RecommendDataCacheSelectStmt(null, 0, new LimitElement(3, 2), null);
        String sql = DataCacheCopilotSQLBuilder.buildCollectStatisticsSQL(stmt.getTarget(), stmt.getInterval(),
                stmt.getLimitElement());
        Assert.assertEquals("SELECT CAST(20 AS INT), \n" +
                "`catalog_name`, \n" +
                "`database_name`, \n" +
                "`table_name`, \n" +
                "`partition_name`, \n" +
                "`column_name`, \n" +
                "`count` \n" +
                "FROM `default_catalog`.`_statistics_`.`datacache_copilot_statistics`\n" +
                "ORDER BY `count` DESC LIMIT 3, 2", sql);

        stmt = new RecommendDataCacheSelectStmt(QualifiedName.of(List.of("catalog"), null), 24 * 3600,
                new LimitElement(3, 2), null);
        sql = DataCacheCopilotSQLBuilder.buildCollectStatisticsSQL(stmt.getTarget(), stmt.getInterval(),
                stmt.getLimitElement());
        Assert.assertTrue(sql.contains("WHERE `access_time` >= from_unixtime"));
        Assert.assertTrue(sql.contains("AND `catalog_name` = 'catalog'"));
        Assert.assertTrue(sql.contains("ORDER BY `count` DESC LIMIT 3, 2"));
    }
}
