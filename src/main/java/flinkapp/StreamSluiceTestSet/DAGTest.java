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
        final long PHASE2_RATE = params.getLong("phase2Rate", 600);
        final long INTERMEDIATE_RATE = params.getLong("interRate", 500);
        final long PHASE1_TIME = params.getLong("phase1Time", 60) * 1000;
        final long PHASE2_TIME = params.getLong("phase2Time", 60) * 1000;
        final long INTERMEDIATE_TIME = params.getLong("interTime", 120) * 1000;
        final long INTERMEDIATE_PERIOD = params.getLong("interPeriod", 240) * 1000;
        final double zipf_skew = params.getDouble("zipf_skew", 0);
        final int nKeys = params.getInt("nkeys", 1000);
        DataStreamSource<Tuple3<String, Long, Long>> source =  env.addSource(new TwoPhaseSineSource(PHASE1_TIME, PHASE2_TIME, INTERMEDIATE_TIME, PHASE1_RATE, PHASE2_RATE, INTERMEDIATE_RATE, INTERMEDIATE_PERIOD,params.getInt("mp2", 8), zipf_skew, nKeys))
                .setParallelism(params.getInt("p1", 1));

        DataStream<Tuple3<String, Long, Long>> up = source
                .keyBy(0)
                .flatMap(new DumbStatefulMap(params.getLong("op2Delay", 100), params.getInt("op2IoRate", 1), params.getInt("op2KeyStateSize", 1)))
                .disableChaining()
                .name("Splitter")
                .uid("op2")
                .setParallelism(params.getInt("p2", 1))
                .setMaxParallelism(params.getInt("mp2", 8))
                .slotSharingGroup("g2");

        DataStream<Tuple3<String, Long, Long>> stream1 = up
                .keyBy(0)
                .flatMap(new DumbStatefulMap(params.getLong("op3Delay", 100), params.getInt("op3IoRate", 1), params.getInt("op3KeyStateSize", 1)))
                .disableChaining()
                .name("FlatMap 3")
                .uid("op3")
                .setParallelism(params.getInt("p3", 1))
                .setMaxParallelism(params.getInt("mp3", 8))
                .slotSharingGroup("g3");

        DataStream<Tuple3<String, Long, Long>> stream2 = up
                .keyBy(0)
                .flatMap(new DumbStatefulMap(params.getLong("op4Delay", 100), params.getInt("op4IoRate", 1), params.getInt("op4KeyStateSize", 1)))
                .disableChaining()
                .name("FlatMap 4")
                .uid("op4")
                .setParallelism(params.getInt("p4", 1))
                .setMaxParallelism(params.getInt("mp4", 8))
                .slotSharingGroup("g4");

        DataStream<Tuple3<String, Long, Long>> unionStream =  stream1.union(stream2);
        unionStream.keyBy(0)
                .map(new DumbSink(params.getLong("op5Delay", 100), params.getInt("op5KeyStateSize", 1)))
                .disableChaining()
                .name("FlatMap 5")
                .uid("op5")
                .setParallelism(params.getInt("p5", 1))
                .setMaxParallelism(params.getInt("mp5", 8))
                .slotSharingGroup("g5");

        env.execute();
    }

    public static final class DumbStatefulMap extends RichFlatMapFunction<Tuple3<String, Long, Long>, Tuple3<String, Long, Long>> {

        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private transient MapState<String, String> countMap;
        private int ioRatio;
        private final int perKeyStateSize;
        private long averageDelay = 1000; // micro second

        private final String payload;

        public DumbStatefulMap(long averageDelay, int ioRatio, int perKeyStateSize) {
            this.averageDelay = averageDelay;
            this.ioRatio = ioRatio;
            this.perKeyStateSize = perKeyStateSize;
            this.payload = StringUtils.repeat("A", perKeyStateSize);
        }

        @Override
        public void flatMap(Tuple3<String, Long, Long> input, Collector<Tuple3<String, Long, Long>> out) throws Exception {
            String s = input.f0;
            countMap.put(s, payload);
            for(int i = 0; i < ioRatio; i++) {
                long t = input.f1;
                long id = input.f2;
                out.collect(new Tuple3<String, Long, Long>(s, t, id));
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

    public static final class DumbSink extends RichMapFunction<Tuple3<String, Long, Long>, Tuple4<String, Long, Long, Long>> {

        private transient MapState<String, String> countMap;
        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private final int perKeyStateSize;
        private long averageDelay = 1000; // micro second
        private final String payload;
        DumbSink(long averageDelay, int perKeyStateSize){
            this.averageDelay = averageDelay;
            this.perKeyStateSize = perKeyStateSize;
            this.payload = StringUtils.repeat("A", perKeyStateSize);
        }

        @Override
        public Tuple4<String, Long, Long, Long> map(Tuple3<String, Long, Long> input) throws Exception {
            countMap.put(input.f0, payload);
            delay(averageDelay);
            long currentTime = System.currentTimeMillis();
            System.out.println("GT: " + input.f0 + ", " + currentTime + ", " + (currentTime - input.f1) + ", " + input.f2);
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


    public static final class TwoPhaseSineSource implements SourceFunction<Tuple3<String, Long, Long>>, CheckpointedFunction {
        private long PHASE1_TIME, PHASE2_TIME, INTERMEDIATE_TIME, PHASE1_RATE, PHASE2_RATE, INTERMEDIATE_RATE, INTERMEDIATE_PERIOD, INTERVAL;
        private int count = 0;
        private volatile boolean isRunning = true;

        private transient ListState<Integer> checkpointedCount;

        private int maxParallelism;
        private FastZipfGenerator fastZipfGenerator;
        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private int nKeys;

        private final Map<Integer, List<String>> keyGroupMapping = new HashMap<>();

        public TwoPhaseSineSource(long PHASE1_TIME, long PHASE2_TIME, long INTERMEDIATE_TIME, long PHASE1_RATE, long PHASE2_RATE, long INTERMEDIATE_RATE, long INTERMEDIATE_PERIOD, int maxParallelism, double zipfSkew, int nkeys){
            this.PHASE1_TIME = PHASE1_TIME;
            this.PHASE2_TIME = PHASE2_TIME;
            this.INTERMEDIATE_TIME = INTERMEDIATE_TIME;
            this.PHASE1_RATE = PHASE1_RATE;
            this.PHASE2_RATE = PHASE2_RATE;
            this.INTERMEDIATE_RATE = INTERMEDIATE_RATE;
            this.INTERMEDIATE_PERIOD = INTERMEDIATE_PERIOD;
            this.INTERVAL = 50;
            this.nKeys = nkeys;
            this.maxParallelism = maxParallelism;
            this.fastZipfGenerator = new FastZipfGenerator(maxParallelism, zipfSkew, 0, 114514);
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

        public void run(SourceContext<Tuple3<String, Long, Long>> ctx) throws Exception {
            List<String> subKeySet;
            // Phase 1
            long startTime = System.currentTimeMillis();
            System.out.println("Phase 1 start at: " + startTime);
            while (isRunning && System.currentTimeMillis() - startTime < PHASE1_TIME) {
                long emitStartTime = System.currentTimeMillis();
                for (int i = 0; i < PHASE1_RATE / 20; i++) {
                    int selectedKeygroup = fastZipfGenerator.next();
                    subKeySet = keyGroupMapping.get(selectedKeygroup);
                    String key = getSubKeySetChar(count, subKeySet);
                    ctx.collect(Tuple3.of(key, System.currentTimeMillis(), (long)count));
                    count++;
                }
                Util.pause(emitStartTime);
            }

            if (!isRunning) {
                return ;
            }

            // Intermediate Phase
            startTime = System.currentTimeMillis();
            System.out.println("Intermediate phase start at: " + startTime);
            long remainedNumber = (long)Math.floor(PHASE1_RATE * INTERVAL / 1000.0);
            long AMPLITUDE = INTERMEDIATE_RATE - PHASE1_RATE;
            while (isRunning && System.currentTimeMillis() - startTime < INTERMEDIATE_TIME) {
                if(remainedNumber <= 0){
                    long index = (System.currentTimeMillis() - startTime) / INTERVAL;
                    long ntime = (index + 1) * INTERVAL + startTime;
                    double theta = Math.sin(Math.toRadians(index * INTERVAL * 360 / ((double)INTERMEDIATE_PERIOD) - 90));
                    remainedNumber = (long)Math.floor((INTERMEDIATE_RATE + theta * AMPLITUDE) / 1000 * INTERVAL);
                    long ctime = System.currentTimeMillis();
                    if(ntime >= ctime) {
                        Thread.sleep(ntime - ctime);
                    }
                }
                synchronized (ctx.getCheckpointLock()) {
                    int selectedKeygroup = fastZipfGenerator.next();
                    subKeySet = keyGroupMapping.get(selectedKeygroup);
                    String key = getSubKeySetChar(count, subKeySet);
                    ctx.collect(Tuple3.of(key, System.currentTimeMillis(), (long)count));
                    remainedNumber --;
                    count++;
                }
            }
            if (!isRunning) {
                return ;
            }

            // Phase 2
            startTime = System.currentTimeMillis();
            System.out.println("Phase 2 start at: " + startTime);
            while (isRunning && System.currentTimeMillis() - startTime < PHASE2_TIME) {
                long emitStartTime = System.currentTimeMillis();
                for (int i = 0; i < PHASE2_RATE / 20; i++) {
                    int selectedKeygroup = fastZipfGenerator.next();
                    subKeySet = keyGroupMapping.get(selectedKeygroup);
                    String key = getSubKeySetChar(count, subKeySet);
                    ctx.collect(Tuple3.of(key, System.currentTimeMillis(), (long)count));
                    count++;
                }
                Util.pause(emitStartTime);
            /*synchronized (ctx.getCheckpointLock()) {
                ctx.collect(Tuple2.of(getChar(count), System.currentTimeMillis()));
                count++;
            }
            if (count % (PHASE2_RATE * INTERVAL / 1000) == 0) {
                long ctime = System.currentTimeMillis();
                long nextTime = ((ctime - startTime) / INTERVAL + 1) * INTERVAL + startTime;
                if(nextTime >= ctime){
                    Thread.sleep(nextTime - ctime);
                }
            }*/
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
