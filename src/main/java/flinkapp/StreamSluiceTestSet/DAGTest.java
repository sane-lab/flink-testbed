package flinkapp.StreamSluiceTestSet;

import Nexmark.sources.Util;
import common.FastZipfGenerator;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MathUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DAGTest {
    public static void main(String[] args) throws Exception {
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        env.setStateBackend(new MemoryStateBackend(1073741824));
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        final long PHASE1_RATE = params.getLong("phase1Rate", 400);
        final long PHASE1_TIME = params.getLong("phase1Time", 60) * 1000;
        final int nKeys = params.getInt("nkeys", 1000);
        DataStreamSource<Tuple4<String, Long, Long, String>> source =  env.addSource(new SimpleSource(PHASE1_TIME, PHASE1_RATE, params.getInt("mp2", 128), nKeys))
                .setParallelism(params.getInt("p1", 1));

        DataStream<Tuple4<String, Long, Long, String>> up = source
                .keyBy(0)
                .flatMap(new DumbStatefulMap("Splitter", params.getLong("op2Delay", 100), params.getInt("op2IoRate", 1), params.getInt("op2KeyStateSize", 1)))
                .disableChaining()
                .name("Splitter")
                .uid("op2")
                .setParallelism(params.getInt("p2", 1))
                .setMaxParallelism(params.getInt("mp2", 8))
                .slotSharingGroup("g2");

        DataStream<Tuple4<String, Long, Long, String>> stream1 = up
                .keyBy(0)
                .flatMap(new DumbStatefulMap("FlatMap 3", params.getLong("op3Delay", 100), params.getInt("op3IoRate", 1), params.getInt("op3KeyStateSize", 1)))
                .disableChaining()
                .name("FlatMap 3")
                .uid("op3")
                .setParallelism(params.getInt("p3", 1))
                .setMaxParallelism(params.getInt("mp3", 8))
                .slotSharingGroup("g3");

        DataStream<Tuple4<String, Long, Long, String>> stream2 = up
                .keyBy(0)
                .flatMap(new DumbStatefulMap("FlatMap 4", params.getLong("op4Delay", 100), params.getInt("op4IoRate", 1), params.getInt("op4KeyStateSize", 1)))
                .disableChaining()
                .name("FlatMap 4")
                .uid("op4")
                .setParallelism(params.getInt("p4", 1))
                .setMaxParallelism(params.getInt("mp4", 8))
                .slotSharingGroup("g4");

        DataStream<Tuple4<String, Long, Long, String>> unionStream =  stream1.union(stream2);
        unionStream.keyBy(0)
                .map(new DumbSink("FlatMap 5", params.getLong("op5Delay", 100), params.getInt("op5KeyStateSize", 1)))
                .disableChaining()
                .name("FlatMap 5")
                .uid("op5")
                .setParallelism(params.getInt("p5", 1))
                .setMaxParallelism(params.getInt("mp5", 8))
                .slotSharingGroup("g5");

        env.execute();
    }

    public static final class DumbStatefulMap extends RichFlatMapFunction<Tuple4<String, Long, Long, String>, Tuple4<String, Long, Long, String>> {

        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private transient MapState<String, String> countMap;
        private int ioRatio;
        private final int perKeyStateSize;
        private long averageDelay = 1000; // micro second

        private long lastReportTime = 0;
        private long processCount = 0;
        private final Map<String, Long> processCountPerUpstreamOperator = new HashMap<>();
        private final String payload, operatorName;

        public DumbStatefulMap(String operatorName, long averageDelay, int ioRatio, int perKeyStateSize) {
            this.operatorName = operatorName;
            this.averageDelay = averageDelay;
            this.ioRatio = ioRatio;
            this.perKeyStateSize = perKeyStateSize;
            this.payload = StringUtils.repeat("A", perKeyStateSize);
        }

        @Override
        public void flatMap(Tuple4<String, Long, Long, String> input, Collector<Tuple4<String, Long, Long, String>> out) throws Exception {
            String s = input.f0;
            countMap.put(s, payload);
            for(int i = 0; i < ioRatio; i++) {
                long t = input.f1;
                long id = input.f2;
                out.collect(new Tuple4<String, Long, Long, String>(s, t, id, operatorName));
                processCount++;
                processCountPerUpstreamOperator.put(operatorName, processCountPerUpstreamOperator.getOrDefault(operatorName, 0L) + 1);
            }
            long ctime = System.currentTimeMillis();
            if(ctime - lastReportTime >= 1000){
                System.out.println(operatorName + " time: " + ctime + " in: " + (ctime - lastReportTime) + " totalProcessed: " + processCount + " per upstream operator: " + processCountPerUpstreamOperator);
                processCount = 0;
                processCountPerUpstreamOperator.clear();
                lastReportTime = ctime;
            }
            delay(averageDelay);
        }

        private void delay(long interval) {
            Double ranN = randomGen.nextGaussian(interval, 1);
            ranN = ranN*1000;
            long delay = ranN.intValue();
            if (delay < 0) delay = interval * 1000;
            Long start = System.nanoTime();
            while (System.nanoTime() - start < delay) {}
        }

        @Override
        public void open(Configuration config) {
            MapStateDescriptor<String, String> descriptor =
                    new MapStateDescriptor<>("word-count", String.class, String.class);
            countMap = getRuntimeContext().getMapState(descriptor);
        }

    }

    public static final class DumbSink extends RichMapFunction<Tuple4<String, Long, Long, String>, Tuple4<String, Long, Long, Long>> {

        private transient MapState<String, String> countMap;
        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private final int perKeyStateSize;
        private long averageDelay = 1000; // micro second
        private final String payload,  operatorName;
        private long lastReportTime = 0;
        private long processCount = 0;
        private final Map<String, Long> processCountPerUpstreamOperator = new HashMap<>();
        DumbSink(String operatorName, long averageDelay, int perKeyStateSize){
            this.operatorName = operatorName;
            this.averageDelay = averageDelay;
            this.perKeyStateSize = perKeyStateSize;
            this.payload = StringUtils.repeat("A", perKeyStateSize);
        }

        @Override
        public Tuple4<String, Long, Long, Long> map(Tuple4<String, Long, Long, String> input) throws Exception {
            countMap.put(input.f0, payload);
            delay(averageDelay);
            long currentTime = System.currentTimeMillis();
            // System.out.println("GT: " + input.f0 + ", " + currentTime + ", " + (currentTime - input.f1) + ", " + input.f2);
            processCount++;
            processCountPerUpstreamOperator.put(operatorName, processCountPerUpstreamOperator.getOrDefault(operatorName, 0L) + 1);
            long ctime = System.currentTimeMillis();
            if(ctime - lastReportTime >= 1000){
                System.out.println(operatorName + " time: " + ctime + " in: " + (ctime - lastReportTime) + " totalProcessed: " + processCount + " per upstream operator: " + processCountPerUpstreamOperator);
                processCount = 0;
                processCountPerUpstreamOperator.clear();
                lastReportTime = ctime;
            }
            return new Tuple4<String, Long, Long, Long>(input.f0, currentTime, currentTime - input.f1, input.f2);
        }
        private void delay(long interval) {
            Double ranN = randomGen.nextGaussian(interval, 1);
            ranN = ranN*1000;
            long delay = ranN.intValue();
            if (delay < 0) delay = interval * 1000;
            Long start = System.nanoTime();
            while (System.nanoTime() - start < delay) {}
        }

        @Override
        public void open(Configuration config) {
            MapStateDescriptor<String, String> descriptor =
                    new MapStateDescriptor<>("word-count", String.class, String.class);
            countMap = getRuntimeContext().getMapState(descriptor);
        }
    }


    public static final class SimpleSource implements SourceFunction<Tuple4<String, Long, Long, String>>, CheckpointedFunction {
        private long PHASE1_TIME, PHASE1_RATE;
        private int count = 0;
        private volatile boolean isRunning = true;

        private transient ListState<Integer> checkpointedCount;

        private int maxParallelism;
        private FastZipfGenerator fastZipfGenerator;
        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private int nKeys;

        private final Map<Integer, List<String>> keyGroupMapping = new HashMap<>();

        public SimpleSource(long PHASE1_TIME, long PHASE1_RATE, int maxParallelism, int nkeys){
            this.PHASE1_TIME = PHASE1_TIME;
            this.PHASE1_RATE = PHASE1_RATE;
            this.nKeys = nkeys;
            this.maxParallelism = maxParallelism;
            this.fastZipfGenerator = new FastZipfGenerator(maxParallelism, 0, 0, 114514);
            for (int i = 0; i < nkeys; i++) {
                String key = getChar(i);
                int keygroup = MathUtils.murmurHash(key.hashCode()) % maxParallelism;
                List<String> keys = keyGroupMapping.computeIfAbsent(keygroup, t -> new ArrayList<>());
                keys.add(key);
            }
        }
        @Override
        public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
            this.checkpointedCount.clear();
            this.checkpointedCount.add(count);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            this.checkpointedCount = context
                    .getOperatorStateStore()
                    .getListState(new ListStateDescriptor<>("checkpointedCount", Integer.class));

            if (context.isRestored()) {
                for (Integer count : this.checkpointedCount.get()) {
                    this.count = count;
                }
            }
        }

        public void run(SourceContext<Tuple4<String, Long, Long, String>> ctx) throws Exception {
            List<String> subKeySet;
            // Phase 1
            long startTime = System.currentTimeMillis();
            long lastMarkCount = 0;
            long lastMarktime = startTime;
            System.out.println("Phase 1 start at: " + startTime);
            while (isRunning && System.currentTimeMillis() - startTime < PHASE1_TIME) {
                long emitStartTime = System.currentTimeMillis();
                for (int i = 0; i < PHASE1_RATE / 20; i++) {
                    int selectedKeygroup = fastZipfGenerator.next();
                    subKeySet = keyGroupMapping.get(selectedKeygroup);
                    String key = getSubKeySetChar(count, subKeySet);
                    ctx.collect(Tuple4.of(key, System.currentTimeMillis(), (long)count, "Source"));
                    count++;
                }
                if(emitStartTime - lastMarktime > 1000){
                    System.out.println("Source time: " + emitStartTime + " rate: " + (count-lastMarkCount));
                    lastMarktime = emitStartTime;
                    lastMarkCount = count;
                }
                Util.pause(emitStartTime);
            }
        }

        private String getChar(int cur) {
            return "A" + (cur % nKeys);
        }


        private String getSubKeySetChar(int cur, List<String> subKeySet) {
            return subKeySet.get(cur % subKeySet.size());
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
