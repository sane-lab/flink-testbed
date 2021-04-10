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

package megaphone.dynamicrules.functions;

import megaphone.dynamicrules.Keyed;
import lombok.extern.slf4j.Slf4j;
import megaphone.dynamicrules.MegaphoneEvaluator;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/** Implements dynamic data partitioning based on a set of broadcasted rules. */
@Slf4j
public class RouterFunction
    extends BroadcastProcessFunction<Tuple2<String, String>, String, Keyed<Tuple2<String, String>, String, String>> {

//  private final Map<Integer, String> globalState = new HashMap<>();
  private final Map<String, String> globalState = new HashMap<>();

  String keyGroupToKeyMapStr = "0=A12, 1=A28, 2=A14, 3=A19, 4=A42, 5=A133, 6=A214, 7=A364, 8=A20, 9=A23, " +
          "10=A9, 11=A203, 12=A145, 13=A163, 14=A234, 15=A7, 16=A33, 17=A175, 18=A40, 19=A164, " +
          "20=A24, 21=A41, 22=A72, 23=A60, 24=A36, 25=A293, 26=A105, 27=A57, 28=A281, 29=A11, " +
          "30=A137, 31=A5, 32=A442, 33=A197, 34=A77, 35=A382, 36=A46, 37=A170, 38=A169, 39=A26, " +
          "40=A168, 41=A139, 42=A108, 43=A179, 44=A62, 45=A4, 46=A249, 47=A48, 48=A147, 49=A64, " +
          "50=A327, 51=A125, 52=A208, 53=A2, 54=A8, 55=A6, 56=A3, 57=A50, 58=A86, 59=A78, " +
          "60=A103, 61=A245, 62=A63, 63=A101, 64=A419, 65=A83, 66=A221, 67=A17, 68=A136, 69=A397, " +
          "70=A226, 71=A0, 72=A94, 73=A354, 74=A44, 75=A156, 76=A10, 77=A148, 78=A55, 79=A106, " +
          "80=A45, 81=A32, 82=A52, 83=A142, 84=A153, 85=A263, 86=A87, 87=A49, 88=A135, 89=A209, " +
          "90=A31, 91=A222, 92=A195, 93=A25, 94=A93, 95=A117, 96=A61, 97=A151, 98=A286, 99=A279, " +
          "100=A67, 101=A18, 102=A1, 103=A251, 104=A229, 105=A227, 106=A21, 107=A357, 108=A91, 109=A76, " +
          "110=A291, 111=A358, 112=A99, 113=A15, 114=A692, 115=A51, 116=A73, 117=A29, 118=A111, 119=A38, " +
          "120=A56, 121=A39, 122=A150, 123=A636, 124=A230, 125=A22, 126=A233, 127=A66";

  Map<Integer, String> keyGroupToKeyMap = new HashMap<>();

  private final String TOPIC = "megaphone_state";	// status topic, used to do recovery.
  private final String servers = "localhost:9092";
  private KafkaConsumer<String, String> consumer;
  private final String uniqueID = UUID.randomUUID().toString();
  // by default all keygroups has false value, set keygroups need to be migrated to true on receiving a new reconfiguration
  private final Map<String, Boolean> affectedKeys = new HashMap<>(128);

  public void initConsumer() {
    final Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, uniqueID);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    consumer = new KafkaConsumer(props);
    consumer.subscribe(Arrays.asList(TOPIC));

    ConsumerRecords<String, String> records = consumer.poll(100);
    Set<TopicPartition> assignedPartitions = consumer.assignment();
    for (TopicPartition partition : assignedPartitions) {
      consumer.seek(partition, 0);
      long endPosition = consumer.position(partition);
    }
  }

  @Override
  public void open(Configuration parameters) {
    ControlMessageCounterGauge controlMessageCounterGauge = new ControlMessageCounterGauge();
    getRuntimeContext().getMetricGroup().gauge("numberOfActiveRules", controlMessageCounterGauge);
    // construct a key to keygroup map from string
    for (String kvStr : keyGroupToKeyMapStr.split(", ")) {
      String[] kv = kvStr.split("=");
      keyGroupToKeyMap.put(Integer.parseInt(kv[0]), kv[1]);
    }
    // no reconfiguration are in progress
    for (int i=0; i<128; i++) {
      String key = "A" + i;
      affectedKeys.put(key, false);
    }
    initConsumer();
  }

  @Override
  public void processElement(
      Tuple2<String, String> event, ReadOnlyContext ctx, Collector<Keyed<Tuple2<String, String>, String, String>> out)
      throws Exception {
    ReadOnlyBroadcastState<String, Integer> rulesState =
        ctx.getBroadcastState(MegaphoneEvaluator.Descriptors.rulesDescriptor);
    forkEventForEachGroupingKey(event, rulesState, out);
  }

  private void forkEventForEachGroupingKey(
      Tuple2<String, String> event,
      ReadOnlyBroadcastState<String, Integer> rulesState,
      Collector<Keyed<Tuple2<String, String>, String, String>> out)
      throws Exception {
    int assignedKeyGroup = rulesState.contains(event.f0) ? rulesState.get(event.f0) : -1;
    String assignedKey = assignedKeyGroup == -1 ? event.f0 : keyGroupToKeyMap.get(assignedKeyGroup);
    if(affectedKeys.get(event.f0)) {
      // if a new version control message is received, the tuple of each keygroup should be attached with the corresponding key state.
      affectedKeys.put(event.f0, false);
      out.collect(new Keyed<>(event, assignedKey, globalState.get(event.f0)));
    } else {
      out.collect(new Keyed<>(event, assignedKey, null));
    }

  }

  @Override
  public void processBroadcastElement(
          String controlMessage, Context ctx, Collector<Keyed<Tuple2<String, String>, String, String>> out) throws Exception {
    log.info("{}", controlMessage);
    BroadcastState<String, Integer> broadcastState =
        ctx.getBroadcastState(MegaphoneEvaluator.Descriptors.rulesDescriptor);
    for (String kvStr : controlMessage.split(", ")) {
      String[] kv = kvStr.split("=");
      broadcastState.put(kv[0], Integer.parseInt(kv[1]));
    }
    // a new reconfiguration is in progress
    for (int i=0; i<128; i++) {
      String key = "A" + i;
      affectedKeys.put(key, true);
    }
    // get the current global state
    long duration = 1000;
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < duration) {
      synchronized (consumer) {
        ConsumerRecords<String, String> records = consumer.poll(100);
        if (!records.isEmpty()) {
          for (ConsumerRecord<String, String> record : records) {
            globalState.put(record.key(), record.value());
          }
        }
      }
    }
    System.out.println(globalState);
  }

  private static class ControlMessageCounterGauge implements Gauge<Integer> {

    private int value = 0;

    public void setValue(int value) {
      this.value = value;
    }

    @Override
    public Integer getValue() {
      return value;
    }
  }
}
