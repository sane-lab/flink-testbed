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

import megaphone.dynamicrules.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Optional;
import java.util.Set;

/** Implements main rule evaluation and alerting logic. */
@Slf4j
public class ProcessorFunction
    extends KeyedBroadcastProcessFunction<
        String, Keyed<Transaction, String, Integer>, String, Alert> {

  @Override
  public void open(Configuration parameters) {

    Meter alertMeter = new MeterView(60);
    getRuntimeContext().getMetricGroup().meter("alertsPerSecond", alertMeter);
  }

  @Override
  public void processElement(
      Keyed<Transaction, String, Integer> value, ReadOnlyContext ctx, Collector<Alert> out)
      throws Exception {

    String controlMessage = ctx.getBroadcastState(MegaphoneEvaluator.Descriptors.rulesDescriptor).get(value.getKey());

    out.collect(
            new Alert<>(
                    0, controlMessage, value.getKey(), value.getWrapped(), 0));
  }

  @Override
  public void processBroadcastElement(String controlMessage, Context ctx, Collector<Alert> out)
      throws Exception {
    log.info("{}", controlMessage);
    BroadcastState<String, String> broadcastState =
        ctx.getBroadcastState(MegaphoneEvaluator.Descriptors.rulesDescriptor);
    broadcastState.put(controlMessage, controlMessage);
  }

//  @Override
//  public void onTimer(final long timestamp, final OnTimerContext ctx, final Collector<Alert> out)
//      throws Exception {
//
//    ControlMessage widestWindowControlMessage = ctx.getBroadcastState(MegaphoneEvaluator.Descriptors.rulesDescriptor).get(WIDEST_RULE_KEY);
//
//    Optional<Long> cleanupEventTimeWindow =
//        Optional.ofNullable(widestWindowControlMessage).map(ControlMessage::getWindowMillis);
//    Optional<Long> cleanupEventTimeThreshold =
//        cleanupEventTimeWindow.map(window -> timestamp - window);
//
//    cleanupEventTimeThreshold.ifPresent(this::evictAgedElementsFromWindow);
//  }

}
