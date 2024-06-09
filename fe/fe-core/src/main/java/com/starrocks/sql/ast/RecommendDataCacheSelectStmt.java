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

import com.starrocks.analysis.LimitElement;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.parser.NodePosition;

public class RecommendDataCacheSelectStmt extends ShowStmt {
    private final QualifiedName target;
    // only interval > 0 is valid
    private final long interval;
    private final LimitElement limitElement;

    public RecommendDataCacheSelectStmt(QualifiedName target, long interval, LimitElement limitElement, NodePosition pos) {
        super(pos);
        this.target = target;
        this.interval = interval;
        this.limitElement = limitElement;
    }

    public QualifiedName getTarget() {
        return target;
    }

    public long getInterval() {
        return interval;
    }

    public LimitElement getLimitElement() {
        return limitElement;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitRecommendDataCacheSelectStmt(this, context);
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return null;
    }
}
