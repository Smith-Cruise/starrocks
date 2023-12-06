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

package com.starrocks.sql.common;

public class OptimizerSetting {
    private final boolean reuseViewDef;
    private final boolean isDataCacheSample;
    private final boolean isDataCacheWarmup;

    public boolean isReuseViewDef() {
        return reuseViewDef;
    }

    public boolean isDataCacheSample() {
        return isDataCacheSample;
    }

    public boolean isDataCacheWarmup() {
        return isDataCacheWarmup;
    }

    private OptimizerSetting(Builder builder) {
        this.reuseViewDef = builder.reuseViewDef;
        this.isDataCacheSample = builder.isDataCacheSample;
        this.isDataCacheWarmup = builder.isDataCacheWarmup;
    }

    public static OptimizerSetting.Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private boolean reuseViewDef;

        private boolean isDataCacheSample = false;
        private boolean isDataCacheWarmup = false;

        public Builder setReuseViewDef(boolean reuseViewDef) {
            this.reuseViewDef = reuseViewDef;
            return this;
        }

        public Builder setDataCacheWarmup(boolean dataCacheWarmup) {
            isDataCacheWarmup = dataCacheWarmup;
            return this;
        }

        public Builder setDataCacheSample(boolean dataCacheSample) {
            isDataCacheSample = dataCacheSample;
            return this;
        }

        public OptimizerSetting build() {
            return new OptimizerSetting(this);
        }
    }
}
