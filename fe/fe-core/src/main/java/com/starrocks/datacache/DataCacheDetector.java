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
import com.starrocks.common.util.ProfileManager;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.common.OptimizerSetting;

import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DataCacheDetector {
    private final ConnectContext connectContext;

    private final String warmupSQL;

    public DataCacheDetector(String sql, String catalog, String database) {
        this.warmupSQL = sql;
        connectContext = new ConnectContext(null);
        connectContext.setCurrentCatalog(catalog);
        connectContext.setDatabase(database);
        connectContext.setQualifiedUser(AuthenticationMgr.ROOT_USER);
        connectContext.setCurrentUserIdentity(
                UserIdentity.createAnalyzedUserIdentWithIp(AuthenticationMgr.ROOT_USER, "%"));
        connectContext.setCurrentRoleIds(connectContext.getCurrentUserIdentity());
        connectContext.getSessionVariable().setEnableAsyncProfile(false);
        connectContext.getSessionVariable().setEnableProfile(true);
        connectContext.getSessionVariable().setEnablePopulateDatacache(true);
        connectContext.getSessionVariable().setEnableScanDataCache(true);
    }

    public long getEstimateSize() throws Exception {
        long sampleSize = getSampleSize();
        return sampleSize;
    }

    private long getSampleSize() throws Exception {
        OptimizerSetting optimizerSetting = OptimizerSetting.builder().setDataCacheSample(true).build();
        connectContext.setOptimizerSetting(optimizerSetting);
        UUID uuid = UUID.randomUUID();
        connectContext.setQueryId(uuid);
        String sampleSQL = String.format("INSERT INTO blackhole() %s LIMIT 100", warmupSQL);
        StmtExecutor stmtExecutor = connectContext.executeSql(sampleSQL);
        String profile = ProfileManager.getInstance().getProfile(uuid.toString());
        // 正则表达式匹配 "- AppIOBytesRead:" 后面的数值和单位
        String regex = "- AppIOBytesRead:\\s*(\\d+\\.\\d+\\s*B)";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(profile);

        if (matcher.find()) {
            String value = matcher.group(1); // 提取数值和单位
            System.out.println(value); // 输出：96.000 B
        } else {
            System.out.println("No match found.");
        }

        if (!stmtExecutor.getCoordinator().isDone()) {
            throw new Exception("failed");
        }

        long dataCacheWarmupBytes = stmtExecutor.getCoordinator().getDataCacheWarmupBytes();

        return dataCacheWarmupBytes;
    }

    private long getTotalScanSize() throws Exception {
        return 0;
    }
}
