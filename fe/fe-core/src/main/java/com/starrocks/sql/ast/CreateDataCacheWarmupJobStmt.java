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

package com.starrocks.sql.ast;

import com.starrocks.sql.parser.NodePosition;

import java.util.Map;

public class CreateDataCacheWarmupJobStmt extends DdlStmt {
    private final boolean isSyncMode;
    private final Map<String, String> properties;
    private final QueryStatement queryStatement;
    private final int startIndex;
    private final int endIndex;

    public CreateDataCacheWarmupJobStmt(QueryStatement queryStatement, int startIndex, int endIndex, boolean isSyncMode,
                                        Map<String, String> properties, NodePosition pos) {
        super(pos);
        this.queryStatement = queryStatement;
        this.startIndex = startIndex;
        this.endIndex = endIndex;
        this.isSyncMode = isSyncMode;
        this.properties = properties;
    }

    public QueryStatement getQueryStatement() {
        return queryStatement;
    }

    public boolean isSyncMode() {
        return isSyncMode;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getQuerySql() {
        return getOrigStmt().originStmt.substring(startIndex, endIndex + 1);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateDataCacheWarmupJobStatement(this, context);
    }
}
