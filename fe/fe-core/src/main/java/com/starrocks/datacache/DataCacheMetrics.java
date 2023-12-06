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

import com.starrocks.thrift.TDataCacheMetrics;

public class DataCacheMetrics {
    public enum Status {

        NORMAL("Normal"), UPDATING("Updating"), ABNORMAL("Abnormal");

        private final String name;

        Status(String name) {
            this.name = name;
        }

        public String getName() {
            return this.name;
        }
    }

    private final Status status;
    private final long memQuoteBytes;
    private final long memUsedBytes;
    private final long diskQuotaBytes;
    private final long diskUsedBytes;
    private final long metaUsedBytes;

    private DataCacheMetrics(Status status, long memQuoteBytes, long memUsedBytes, long diskQuotaBytes,
                             long diskUsedBytes,
                             long metaUsedBytes) {
        this.status = status;
        this.memQuoteBytes = memQuoteBytes;
        this.memUsedBytes = memUsedBytes;
        this.diskQuotaBytes = diskQuotaBytes;
        this.diskUsedBytes = diskUsedBytes;
        this.metaUsedBytes = metaUsedBytes;
    }

    public static DataCacheMetrics buildFromThrift(TDataCacheMetrics tMetrics) {
        Status status = Status.ABNORMAL;
        if (tMetrics.isSetStatus()) {
            switch (tMetrics.status) {
                case NORMAL:
                    status = Status.NORMAL;
                    break;
                case UPDATING:
                    status = Status.UPDATING;
                    break;
                default:
            }
        }

        long memQuoteBytes = tMetrics.isSetMem_quota_bytes() ? tMetrics.mem_quota_bytes : 0;
        long memUsedBytes = tMetrics.isSetMem_used_bytes() ? tMetrics.mem_used_bytes : 0;
        long diskQuotaBytes = tMetrics.isSetDisk_quota_bytes() ? tMetrics.disk_quota_bytes : 0;
        long diskUsedBytes = tMetrics.isSetDisk_used_bytes() ? tMetrics.disk_used_bytes : 0;
        long metaUsedBytes = tMetrics.isSetMeta_used_bytes() ? tMetrics.meta_used_bytes : 0;


        return new DataCacheMetrics(status, memQuoteBytes, memUsedBytes, diskQuotaBytes, diskUsedBytes, metaUsedBytes);
    }

    public String getMemUsage() {
        return String.format("%s/%s", convertSize(memUsedBytes), convertSize(memQuoteBytes));
    }

    public String getDiskUsage() {
        return String.format("%s/%s", convertSize(diskUsedBytes), convertSize(diskQuotaBytes));
    }

    public String getMetaUsage() {
        return String.format("%s", convertSize(metaUsedBytes));
    }

    public Status getStatus() {
        return status;
    }

    private static String convertSize(long size) {
        double base1Gigabytes = 1024 * 1024 * 1024;
        double res = size / base1Gigabytes;
        return String.format("%.2fGb", res);
    }
}
