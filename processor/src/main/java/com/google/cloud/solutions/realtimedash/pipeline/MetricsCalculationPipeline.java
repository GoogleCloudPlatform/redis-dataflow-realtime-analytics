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

package com.google.cloud.solutions.realtimedash.pipeline;

import com.google.common.flogger.FluentLogger;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.redis.RedisIO;
import org.apache.beam.sdk.io.redis.RedisIO.Write.Method;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Realtime Dataflow pipeline to extract experiment metrics from Log Events published on Pub/Sub.
 */
public final class MetricsCalculationPipeline {

  private static final FluentLogger logger = FluentLogger.forEnclosingClass();
  private static final long DEFAULT_WINDOW_DURATION = 1L; // 1 - second

  /**
   * Parses the command line arguments and runs the pipeline.
   */
  public static void main(String[] args) {
    MetricsPipelineOptions options = extractPipelineOptions(args);
    Pipeline pipeline = Pipeline.create(options);

    PCollection<LogEvent> parsedLoggedEvents =
        pipeline
            .apply("Read PubSub Events",
                PubsubIO.readStrings().fromTopic(options.getInputTopic()))
            .apply("Parse Message JSON",
                ParDo.of(new ParseMessageAsLogElement()));

    RedisIO.Write redisWriter =
        RedisIO.write()
            .withEndpoint(
                options.getRedisHost(), options.getRedisPort());

    //visit counter
    parsedLoggedEvents
        .apply("Count visits per minute", ParDo.of(
            new DoFn<LogEvent, KV<String, String>>() {
              @ProcessElement
              public void countSession(ProcessContext context) {
                LogEvent event = context.element();
                context.output(
                    KV.of(event.getTimestamp().toString(timeBasedKeyBuilder("visitCounter")), "1"));
              }
            }
        ))
        .apply("Update Visit counter", redisWriter.withMethod(Method.INCRBY));

    // Build user - experiment/variant metric
    parsedLoggedEvents
        .apply("extract user for experiment-variant per minute metric",
            ParDo.of(new DoFn<LogEvent, KV<String, String>>() {

              @ProcessElement
              public void extractExperimentVariantPerTime(ProcessContext context) {
                LogEvent event = context.element();

                String key = event.getTimestamp().toString(timeBasedKeyBuilder(
                    "evcounter_e_" + event.getExperimentId() + "_v_" + event.getVariant()));
                context.output(KV.of(key, event.getUid()));
              }
            }))
        .apply("Update EV Counters", redisWriter.withMethod(Method.PFADD));

    // Variant based metrics
    PCollection<Pair<String, String>> variants =
        parsedLoggedEvents
            .apply("Extract Users per Variant",
                ParDo.of(
                    new DoFn<LogEvent, Pair<String, String>>() {
                      @ProcessElement
                      public void extractVariantAndUser(ProcessContext context) {
                        LogEvent elem = context.element();
                        context.output(Pair.of("" + elem.getVariant(), elem.getUid()));
                      }
                    }));
    variants
        .apply("Build HLL Keys (Variant)", hllKeyGenerator("var"))
        .apply("Add user to Variant HLL", redisWriter.withMethod(Method.PFADD));

    variants
        .apply("Build Set Keys (Variant)", setKeyGenerator("var"))
        .apply("Add user to Variant set", redisWriter.withMethod(Method.SADD));

    // Experiment based metrics
    PCollection<Pair<String, String>> experiments =
        parsedLoggedEvents
            .apply("Extract Users per Experiment",
                ParDo.of(
                    new DoFn<LogEvent, Pair<String, String>>() {
                      @ProcessElement
                      public void extractExperimentKvPair(ProcessContext context) {
                        LogEvent elem = context.element();
                        context.output(Pair.of(elem.getExperimentId(), elem.getUid()));
                      }
                    }
                ));

    experiments
        .apply("Build HLL Keys (Experiment)", hllKeyGenerator("exp"))
        .apply("Add User to Experiment HLL", redisWriter.withMethod(Method.PFADD));

    experiments
        .apply("Build Set Keys (Experiment)", setKeyGenerator("exp"))
        .apply("Add user to Experiment set", redisWriter.withMethod(Method.SADD));

    PCollection<Pair<String, String>> activeExperiments =
        parsedLoggedEvents
            .apply("Build Time bound experiments",
                ParDo.of(new DoFn<LogEvent, Pair<String, String>>() {

                  @ProcessElement
                  public void extractExperimentForTime(ProcessContext context) {
                    LogEvent event = context.element();

                    context.output(Pair.of(
                        event.getTimestamp().toString(
                            timeBasedKeyBuilder("experiments")),
                        event.getExperimentId()));

                  }
                }));

    activeExperiments
        .apply("build experiments hll key", hllKeyGenerator("experiments"))
        .apply("Write Active experiments data",
            redisWriter.withMethod(Method.PFADD));

    activeExperiments
        .apply("build experiments set key", setKeyGenerator("experiments"))
        .apply("Write Active experiments count", redisWriter.withMethod(Method.SADD));

    PCollection<Pair<String, String>> activeVariants =
        parsedLoggedEvents
            .apply("Build Time bound Variants",
                ParDo.of(new DoFn<LogEvent, Pair<String, String>>() {

                  @ProcessElement
                  public void extractExperimentForTime(ProcessContext context) {
                    LogEvent event = context.element();

                    context.output(Pair.of(
                        event.getTimestamp().toString(
                            timeBasedKeyBuilder("variants")),
                        event.getVariant()));
                  }
                }));

    activeVariants
        .apply("build variants hll key", hllKeyGenerator("variants"))
        .apply("Write Active experiments data",
            redisWriter.withMethod(Method.PFADD));

    activeVariants
        .apply("build variants set key", setKeyGenerator("variants"))
        .apply("Write Active experiments count", redisWriter.withMethod(Method.SADD));

    // Date_Hour_Minute based metrics
    PCollection<Pair<String, String>> datesHoursBasedEvents =
        parsedLoggedEvents
            .apply("Extract Users per minute", extractUsersForDateTime());

    datesHoursBasedEvents
        .apply("Build HLL Keys (Date_Time)", hllKeyGenerator("dthr"))
        .apply("Add User to DateTime HLL", redisWriter.withMethod(Method.PFADD));

    datesHoursBasedEvents
        .apply("Build Set Keys (Date_Time)", setKeyGenerator("dthr"))
        .apply("Add User to DateTime set", redisWriter.withMethod(Method.SADD));

    pipeline.run();
  }

  private static SingleOutput<Pair<String, String>, KV<String, String>>
  hllKeyGenerator(String name) {
    return
        ParDo.of(new DoFn<Pair<String, String>, KV<String, String>>() {
          @ProcessElement
          public void buildHllKey(ProcessContext context) {
            Pair<String, String> elem = context.element();
            context.output(
                KV.of("hll_" + buildPrefix(name) + elem.getKey(), elem.getValue()));
          }
        });
  }

  private static SingleOutput<Pair<String, String>, KV<String, String>> setKeyGenerator(
      String name) {
    return ParDo.of(
        new DoFn<Pair<String, String>, KV<String, String>>() {
          @ProcessElement
          public void buildSetKey(ProcessContext context) {
            Pair<String, String> elem = context.element();
            context.output(
                KV.of("set_" + buildPrefix(name) + elem.getKey(), elem.getValue()));
          }
        });
  }

  private static SingleOutput<LogEvent, Pair<String, String>> extractUsersForDateTime() {
    return ParDo.of(
        new DoFn<LogEvent, Pair<String, String>>() {
          @ProcessElement
          public void extractDateTimeForUser(ProcessContext context) {
            LogEvent elem = context.element();
            context.output(
                Pair.of(elem.getTimestamp().toString(timeBasedKeyBuilder(null)),
                    elem.getUid()));
          }
        });
  }

  private static String timeBasedKeyBuilder(String prefix) {
    return (prefix == null ? "" : ("'" + buildPrefix(prefix) + "'")) + "yyyy_MM_dd'T'HH_mm";
  }

  private static String buildPrefix(String prefix) {
    return (prefix == null || prefix.equals("")) ? "" : prefix + "_";
  }

  /**
   * Parse Pipeline options from command line arguments.
   */
  private static MetricsPipelineOptions extractPipelineOptions(String[] args) {
    MetricsPipelineOptions options = PipelineOptionsFactory
        .fromArgs(args)
        .withValidation()
        .as(MetricsPipelineOptions.class);

    return options;
  }
}
