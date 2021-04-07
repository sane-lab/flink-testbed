/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package megaphone.dynamicrules.sources;

import megaphone.config.Config;
import megaphone.dynamicrules.ControlMessage;
import megaphone.dynamicrules.KafkaUtils;
import megaphone.dynamicrules.functions.RuleDeserializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.gcp.pubsub.PubSubSource;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static megaphone.config.Parameters.*;

public class ControlMessageSource {

  private static final int RULES_STREAM_PARALLELISM = 1;

  public static SourceFunction<String> createControlMessageSource(Config config) throws IOException {

    String sourceType = config.get(RULES_SOURCE);
    Type rulesSourceType = Type.valueOf(sourceType.toUpperCase());

    switch (rulesSourceType) {
      case KAFKA:
        Properties kafkaProps = KafkaUtils.initConsumerProperties(config);
        String rulesTopic = config.get(RULES_TOPIC);
        FlinkKafkaConsumer011<String> kafkaConsumer =
            new FlinkKafkaConsumer011<>(rulesTopic, new SimpleStringSchema(), kafkaProps);
        kafkaConsumer.setStartFromLatest();
        return kafkaConsumer;
      case PUBSUB:
        return PubSubSource.<String>newBuilder()
            .withDeserializationSchema(new SimpleStringSchema())
            .withProjectName(config.get(GCP_PROJECT_NAME))
            .withSubscriptionName(config.get(GCP_PUBSUB_RULES_SUBSCRIPTION))
            .build();
      case SOCKET:
        return new SocketTextStreamFunction("localhost", config.get(SOCKET_PORT), "\n", -1);
      case CUSTOM:
        return new MySource();
      default:
        throw new IllegalArgumentException(
            "Source \"" + rulesSourceType + "\" unknown. Known values are:" + Type.values());
    }
  }

  public static DataStream<ControlMessage> stringsStreamToRules(DataStream<String> ruleStrings) {
    return ruleStrings
        .flatMap(new RuleDeserializer())
        .name("ControlMessage Deserialization")
        .setParallelism(RULES_STREAM_PARALLELISM)
        .assignTimestampsAndWatermarks(
            new BoundedOutOfOrdernessTimestampExtractor<ControlMessage>(Time.of(0, TimeUnit.MILLISECONDS)) {
              @Override
              public long extractTimestamp(ControlMessage element) {
                // Prevents connected data+update stream watermark stalling.
                return Long.MAX_VALUE;
              }
            });
  }

  public enum Type {
    KAFKA("Rules Source (Kafka)"),
    PUBSUB("Rules Source (Pub/Sub)"),
    SOCKET("Rules Source (Socket)"),
    CUSTOM("Custom source");

    private String name;

    Type(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }

  private static class MySource implements SourceFunction<String> {

    private volatile boolean isRunning = true;


    @Override
    public void run(SourceContext<String> ctx) {
//      String ruleString = "{ \"ruleId\": 1, " +
//              "\"ruleState\": \"ACTIVE\", " +
//              "\"groupingKeyNames\": [\"paymentType\"], " +
//              "\"unique\": [], " +
//              "\"aggregateFieldName\": \"paymentAmount\", " +
//              "\"aggregatorFunctionType\": \"SUM\"," +
//              "\"limitOperatorType\": \"GREATER\"," +
//              "\"limit\": 500, " +
//              "\"windowMinutes\": 20}";

      String payload1 =
              "{\"ruleId\":\"1\","
                      + "\"aggregateFieldName\":\"paymentAmount\","
                      + "\"aggregatorFunctionType\":\"SUM\","
                      + "\"groupingKeyNames\":[\"payeeId\", \"beneficiaryId\"],"
                      + "\"limit\":\"20000000\","
                      + "\"limitOperatorType\":\"GREATER\","
                      + "\"ruleState\":\"ACTIVE\","
                      + "\"windowMinutes\":\"43200\"}";

      String payload2 =
              "{\"ruleId\":\"2\","
                      + "\"aggregateFieldName\":\"COUNT_FLINK\","
                      + "\"aggregatorFunctionType\":\"SUM\","
                      + "\"groupingKeyNames\":[\"paymentType\"],"
                      + "\"limit\":\"300\","
                      + "\"limitOperatorType\":\"LESS\","
                      + "\"ruleState\":\"PAUSE\","
                      + "\"windowMinutes\":\"1440\"}";

      String payload3 =
              "{\"ruleId\":\"3\","
                      + "\"aggregateFieldName\":\"paymentAmount\","
                      + "\"aggregatorFunctionType\":\"SUM\","
                      + "\"groupingKeyNames\":[\"beneficiaryId\"],"
                      + "\"limit\":\"10000000\","
                      + "\"limitOperatorType\":\"GREATER_EQUAL\","
                      + "\"ruleState\":\"ACTIVE\","
                      + "\"windowMinutes\":\"1440\"}";


      String payload4 =
              "{\"ruleId\":\"4\","
                      + "\"aggregateFieldName\":\"COUNT_WITH_RESET_FLINK\","
                      + "\"aggregatorFunctionType\":\"SUM\","
                      + "\"groupingKeyNames\":[\"paymentType\"],"
                      + "\"limit\":\"100\","
                      + "\"limitOperatorType\":\"GREATER_EQUAL\","
                      + "\"ruleState\":\"ACTIVE\","
                      + "\"windowMinutes\":\"1440\"}";

      ctx.collect(payload1);
      ctx.collect(payload2);
      ctx.collect(payload3);
      ctx.collect(payload4);
    }

    @Override
    public void cancel() {
      isRunning = false;
    }
  }
}
