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

public class DataCacheCopilotConstants {
    // constants for datacache_copilot_statistics table
    public static final String DATACACHE_COPILOT_STATISTICS_TABLE_NAME = "datacache_copilot_statistics";
    public static final String CATALOG_COLUMN_NAME = "catalog_name";
    public static final String DATABASE_COLUMN_NAME = "database_name";
    public static final String TABLE_COLUMN_NAME = "table_name";
    public static final String PARTITION_COLUMN_NAME = "partition_name";
    public static final String COLUMN_COLUMN_NAME = "column_name";
    public static final String ACCESS_TIME_COLUMN_NAME = "access_time";
    public static final String COUNT_COLUMN_NAME = "count";
}
