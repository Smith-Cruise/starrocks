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

import java.util.Map;

public class DataCacheDetectRecorder {
    private static DataCacheWarmupMetrics lastWarmupMetrics;
    private static Map<Long, Long> backendToScanRangeSize;

    public static void setBackendToScanRangeSize(Map<Long, Long> backendToScanRangeSize) {
        DataCacheDetectRecorder.backendToScanRangeSize = backendToScanRangeSize;
    }

    public static Map<Long, Long> getBackendToScanRangeSize() {
        return backendToScanRangeSize;
    }

    public static void setLastWarmupMetrics(DataCacheWarmupMetrics lastWarmupMetrics) {
        DataCacheDetectRecorder.lastWarmupMetrics = lastWarmupMetrics;
    }

    public static DataCacheWarmupMetrics getLastWarmupMetrics() {
        return lastWarmupMetrics;
    }

    public static long getTotalSize() {
        long total = 0;
        for (Map.Entry<Long, Long> entry : backendToScanRangeSize.entrySet()) {
            total += entry.getValue();
        }
        return total;
    }
}
