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

package com.starrocks.scheduler;

import com.starrocks.common.util.ProfileManager;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;

public class DataCacheWarmupTaskRunProcessor extends BaseTaskRunProcessor {
    @Override
    public void processTaskRun(TaskRunContext context) throws Exception {
        StmtExecutor executor = null;
        ConnectContext ctx = context.getCtx();
        try {
            ctx.getSessionVariable().setEnableScanDataCache(true);
            ctx.getSessionVariable().setEnablePopulateDatacache(true);
            ctx.getSessionVariable().setEnableProfile(true);
            //            ctx.getAuditEventBuilder().reset();
            //            ctx.getAuditEventBuilder()
            //                    .setTimestamp(System.currentTimeMillis())
            //                    .setClientIp(context.getRemoteIp())
            //                    .setUser(ctx.getQualifiedUser())
            //                    .setDb(ctx.getDatabase())
            //                    .setCatalog(ctx.getCurrentCatalog());
            //            Tracers.register(ctx);
            executor = ctx.executeSql(context.getDefinition());
            String profile = ProfileManager.getInstance().getProfile(ctx.getQueryId().toString());
            System.out.println(profile);
        } catch (Exception e) {
            throw e;
        } finally {
            ctx.getSessionVariable().setEnableProfile(false);
            ctx.getSessionVariable().setEnableScanDataCache(false);
            ctx.getSessionVariable().setEnablePopulateDatacache(false);
            //            Tracers.close();
            //            if (executor != null) {
            //                auditAfterExec(context, executor.getParsedStmt(), executor.getQueryStatisticsForAuditLog());
            //            } else {
            //                // executor can be null if we encounter analysis error.
            //                auditAfterExec(context, null, null);
            //            }
        }
    }
}
