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

package com.ververica.field.dynamicrules.functions;

import com.ververica.field.dynamicrules.ControlMessage;
import com.ververica.field.dynamicrules.Keyed;
import com.ververica.field.dynamicrules.KeysExtractor;
import com.ververica.field.dynamicrules.ControlMessage.ControlType;
import com.ververica.field.dynamicrules.ControlMessage.RuleState;
import com.ververica.field.dynamicrules.MegaphoneEvaluator.Descriptors;
import com.ververica.field.dynamicrules.Transaction;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Map.Entry;

import static com.ververica.field.dynamicrules.functions.ProcessingUtils.handleRuleBroadcast;

/** Implements dynamic data partitioning based on a set of broadcasted rules. */
@Slf4j
public class DynamicKeyFunction
    extends BroadcastProcessFunction<Transaction, ControlMessage, Keyed<Transaction, String, Integer>> {

  private ControlMessageCounterGauge controlMessageCounterGauge;

  @Override
  public void open(Configuration parameters) {
    controlMessageCounterGauge = new ControlMessageCounterGauge();
    getRuntimeContext().getMetricGroup().gauge("numberOfActiveRules", controlMessageCounterGauge);
  }

  @Override
  public void processElement(
      Transaction event, ReadOnlyContext ctx, Collector<Keyed<Transaction, String, Integer>> out)
      throws Exception {
    ReadOnlyBroadcastState<Integer, ControlMessage> rulesState =
        ctx.getBroadcastState(Descriptors.rulesDescriptor);
    forkEventForEachGroupingKey(event, rulesState, out);
  }

  private void forkEventForEachGroupingKey(
      Transaction event,
      ReadOnlyBroadcastState<Integer, ControlMessage> rulesState,
      Collector<Keyed<Transaction, String, Integer>> out)
      throws Exception {
    int ruleCounter = 0;
    for (Entry<Integer, ControlMessage> entry : rulesState.immutableEntries()) {
      final ControlMessage controlMessage = entry.getValue();
      out.collect(
          new Keyed<>(
              event, KeysExtractor.getKey(controlMessage.getGroupingKeyNames(), event), controlMessage.getRuleId()));
      ruleCounter++;
    }
    controlMessageCounterGauge.setValue(ruleCounter);
  }

  @Override
  public void processBroadcastElement(
          ControlMessage controlMessage, Context ctx, Collector<Keyed<Transaction, String, Integer>> out) throws Exception {
    log.info("{}", controlMessage);
    BroadcastState<Integer, ControlMessage> broadcastState =
        ctx.getBroadcastState(Descriptors.rulesDescriptor);
    handleRuleBroadcast(controlMessage, broadcastState);
    if (controlMessage.getRuleState() == RuleState.CONTROL) {
      handleControlCommand(controlMessage.getControlType(), broadcastState);
    }
  }

  private void handleControlCommand(
      ControlType controlType, BroadcastState<Integer, ControlMessage> rulesState) throws Exception {
    switch (controlType) {
      case DELETE_RULES_ALL:
        Iterator<Entry<Integer, ControlMessage>> entriesIterator = rulesState.iterator();
        while (entriesIterator.hasNext()) {
          Entry<Integer, ControlMessage> ruleEntry = entriesIterator.next();
          rulesState.remove(ruleEntry.getKey());
          log.info("Removed ControlMessage {}", ruleEntry.getValue());
        }
        break;
    }
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
