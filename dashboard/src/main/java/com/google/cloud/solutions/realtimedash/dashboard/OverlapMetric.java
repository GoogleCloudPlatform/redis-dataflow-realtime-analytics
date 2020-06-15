/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.solutions.realtimedash.dashboard;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;

@AutoValue
public abstract class OverlapMetric {

  public abstract ImmutableSet<String> getDimensions();

  public abstract Double getMetric();


  public static Builder builder() {
    return new AutoValue_OverlapMetric.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setMetric(Double newMetric);

    public Builder setMetric(float newMetric) {
      return setMetric((double) newMetric);
    }

    public Builder setMetric(int newMetric) {
      return setMetric((double) newMetric);
    }

    public Builder setMetric(long newMetric) {
      return setMetric((double) newMetric);
    }

    public abstract Builder setDimensions(ImmutableSet<String> dimensions);

    public abstract OverlapMetric build();
  }
}
