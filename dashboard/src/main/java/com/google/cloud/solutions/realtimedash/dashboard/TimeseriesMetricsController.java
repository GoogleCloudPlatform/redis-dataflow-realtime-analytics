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

import static com.google.common.collect.ImmutableSet.toImmutableSet;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import redis.clients.jedis.Jedis;

@RestController
@RequestMapping("/metrics/timeseries")
public class TimeseriesMetricsController {

  private static final String REDIS_NIL = "(nil)";
  private static final String DEFAULT_REPORT_TIME_MINUTE = "10";

  @GetMapping("/visits")
  public ImmutableList<TimeSeriesMetric> visits(Jedis redisClient,
      @RequestParam(required = false, defaultValue = DEFAULT_REPORT_TIME_MINUTE) int lastMinutes) {
    DateTime[] times = timePatternForLastMinutes(lastMinutes);

    List<String> values = redisClient
        .mget(TimeSeriesKeyBuilder.forPrefix("visitCounter").buildTimeKeys(times));

    ImmutableList.Builder<TimeSeriesMetric> visitsMetricBuilder = ImmutableList.builder();

    for (int index = 0; index < times.length; index++) {
      int value = getIntValue(values.get(index));

      visitsMetricBuilder
          .add(TimeSeriesMetric.builder().setTimestamp(times[index]).setMetric(value).build());
    }

    return visitsMetricBuilder.build();
  }

  private Integer getIntValue(String value) {
    return (value == null || value.equals(REDIS_NIL)) ? 0 : Integer.valueOf(value);
  }

  @GetMapping("/users")
  public ImmutableList<TimeSeriesMetric> users(Jedis redisClient,
      @RequestParam(required = false, defaultValue = DEFAULT_REPORT_TIME_MINUTE) int lastMinutes) {
    ImmutableList.Builder<TimeSeriesMetric> usersMetricBuilder = ImmutableList.builder();

    TimeSeriesKeyBuilder keyBuilder = TimeSeriesKeyBuilder.forPrefix("hll_dthr");

    for (DateTime time : timePatternForLastMinutes(lastMinutes)) {
      usersMetricBuilder.add(
          TimeSeriesMetric
              .builder()
              .setTimestamp(time)
              .setMetric(redisClient.pfcount(keyBuilder.buildTimeKey(time)))
              .build());
    }

    return usersMetricBuilder.build();
  }

  @GetMapping("/experiments")
  public ImmutableList<TimeSeriesMetric> recentExperiments(Jedis redisClient,
      @RequestParam(required = false, defaultValue = DEFAULT_REPORT_TIME_MINUTE) int lastMinutes) {
    TimeSeriesKeyBuilder keyBuilder = TimeSeriesKeyBuilder
        .forPrefix("set_experiments_experiments");

    ImmutableList.Builder<TimeSeriesMetric> experimentsMetricBuilder = ImmutableList.builder();

    for (DateTime time : timePatternForLastMinutes(lastMinutes)) {
      long value = redisClient.scard(keyBuilder.buildTimeKey(time));

      experimentsMetricBuilder.add(
          TimeSeriesMetric.builder()
              .setTimestamp(time)
              .setMetric(value)
              .build());
    }

    return experimentsMetricBuilder.build();
  }

  @GetMapping("/variantsOverlap")
  public ImmutableSet<OverlapMetric> variantOverlap(Jedis redisClient) {
    Set<String> variants = redisClient.keys("set_var_*");

    if (variants == null || variants.size() == 0) {
      return ImmutableSet.of();
    }

    return Sets.combinations(variants, 2)
        .stream()
        .map(variantCombination -> variantCombination.toArray(new String[0]))
        .map(variantCombination -> {

          String sinterStoreKey = Joiner.on("-").join("overlap_", variantCombination);

          redisClient.sinterstore(sinterStoreKey, variantCombination);

          ImmutableSet<String> variantCombinationSet
              = Arrays.stream(variantCombination).map(name -> name.replaceFirst("set_var_", ""))
              .collect(toImmutableSet());

          return OverlapMetric.builder()
              .setDimensions(variantCombinationSet)
              .setMetric(redisClient.scard(sinterStoreKey))
              .build();
        })
        .collect(toImmutableSet());
  }

  @GetMapping("/times")
  public DateTime[] getTimeString(
      @RequestParam(required = false, defaultValue = "10") Integer lastMinutes) {
    return timePatternForLastMinutes(lastMinutes);
  }

  private static DateTime[] timePatternForLastMinutes(int pastMinutes) {
    DateTime startTime = DateTime.now(DateTimeZone.UTC).minuteOfHour().roundFloorCopy();

    return IntStream.rangeClosed(1, pastMinutes)
        .boxed()
        .map(Duration::standardMinutes)
        .map(startTime::minus)
        .toArray(DateTime[]::new);
  }
}
