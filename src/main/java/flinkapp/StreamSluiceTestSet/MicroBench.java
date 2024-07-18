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
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MathUtils;

import java.util.*;

public class MicroBench {
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
        final long INTERMEDIATE_RANGE = params.getLong("interRange", 250);
        final long PHASE1_TIME = params.getLong("phase1Time", 60) * 1000;
        final long PHASE2_TIME = params.getLong("phase2Time", 60) * 1000;
        final long INTERMEDIATE_TIME = params.getLong("interTime", 120) * 1000;
        final long INTERMEDIATE_PERIOD = params.getLong("interPeriod", 240) * 1000;
        final double zipf_skew = params.getDouble("zipf_skew", 0);
        final int nKeys = params.getInt("nkeys", 1000);
        final String GRAPH_TYPE = params.get("graph", "2op");
        final String SOURCE_TYPE = params.get("source", "normal");

        DataStreamSource<Tuple3<String, Long, Long>> source;
        if(SOURCE_TYPE.equals("when")){
            source = env.addSource(new WhenSource(PHASE1_TIME, PHASE2_TIME, INTERMEDIATE_TIME, PHASE1_RATE, PHASE2_RATE, INTERMEDIATE_RATE, params.getLong("run_time", 510) * 1000)).
                    setParallelism(params.getInt("p1", 1));
        }else if(SOURCE_TYPE.equals("how")) {
            source = env.addSource(new HowSource(PHASE1_TIME, PHASE2_TIME, INTERMEDIATE_TIME, PHASE1_RATE, PHASE2_RATE, INTERMEDIATE_RATE, params.getLong("run_time", 510) * 1000));
        }else {
            source = env.addSource(new DynamicAvgRateSineSource(PHASE1_TIME, PHASE2_TIME, INTERMEDIATE_TIME, PHASE1_RATE, PHASE2_RATE, INTERMEDIATE_RATE, INTERMEDIATE_RANGE, INTERMEDIATE_PERIOD, params.getLong("macroInterAmplitude", 0), params.getLong("macroInterPeriod", 60) * 1000, params.getInt("mp2", 8), zipf_skew, nKeys, params.get("curve_type", "sine"), params.getInt("inter_delta", 0)))
                    .setParallelism(params.getInt("p1", 1));
        }
        if(GRAPH_TYPE.equals("1op")){
            source.keyBy(0)
                    .map(new DumbSink(params.getLong("op2Delay", 100), params.getInt("op2KeyStateSize", 1), params.getBoolean("outputGroundTruth", true)))
                    .disableChaining()
                    .name("FlatMap 2")
                    .uid("op2")
                    .setParallelism(params.getInt("p2", 1))
                    .setMaxParallelism(params.getInt("mp2", 8))
                    .slotSharingGroup("g2");
            env.execute();
            return ;
        }
        SingleOutputStreamOperator<Tuple3<String, Long, Long>> leng1 = source.keyBy(0)
                .flatMap(new DumbStatefulMap(params.getLong("op2Delay", 100), params.getInt("op2IoRate", 1), params.getBoolean("op2IoFix", true), params.getInt("op2KeyStateSize", 1)))
                .disableChaining()
                .name("FlatMap 2")
                .uid("op2")
                .setParallelism(params.getInt("p2", 1))
                .setMaxParallelism(params.getInt("mp2", 8))
                .slotSharingGroup("g2");

        if(GRAPH_TYPE.equals("2op")){
            leng1.keyBy(0).map(new DumbSink(params.getLong("op3Delay", 100), params.getInt("op3KeyStateSize", 1), params.getBoolean("outputGroundTruth", true)))
                    .disableChaining()
                    .name("FlatMap 3")
                    .uid("op3")
                    .setParallelism(params.getInt("p3", 1))
                    .setMaxParallelism(params.getInt("mp3", 8))
                    .slotSharingGroup("g3");
            env.execute();
            return ;
        }else if(GRAPH_TYPE.equals("1split2")){
            leng1.keyBy(0).map(new DumbSink(params.getLong("op3Delay", 100), params.getInt("op3KeyStateSize", 1), params.getBoolean("outputGroundTruth", true)))
                    .disableChaining()
                    .name("FlatMap 3")
                    .uid("op3")
                    .setParallelism(params.getInt("p3", 1))
                    .setMaxParallelism(params.getInt("mp3", 8))
                    .slotSharingGroup("g3");
            leng1.keyBy(0).map(new DumbSink(params.getLong("op4Delay", 100), params.getInt("op4KeyStateSize", 1), params.getBoolean("outputGroundTruth", true)))
                    .disableChaining()
                    .name("FlatMap 4")
                    .uid("op4")
                    .setParallelism(params.getInt("p4", 1))
                    .setMaxParallelism(params.getInt("mp4", 8))
                    .slotSharingGroup("g4");
            env.execute();
            return ;
        }else if(GRAPH_TYPE.equals("1split3")){
            leng1.keyBy(0).map(new DumbSink(params.getLong("op3Delay", 100), params.getInt("op3KeyStateSize", 1), params.getBoolean("outputGroundTruth", true)))
                    .disableChaining()
                    .name("FlatMap 3")
                    .uid("op3")
                    .setParallelism(params.getInt("p3", 1))
                    .setMaxParallelism(params.getInt("mp3", 8))
                    .slotSharingGroup("g3");
            leng1.keyBy(0).map(new DumbSink(params.getLong("op4Delay", 100), params.getInt("op4KeyStateSize", 1), params.getBoolean("outputGroundTruth", true)))
                    .disableChaining()
                    .name("FlatMap 4")
                    .uid("op4")
                    .setParallelism(params.getInt("p4", 1))
                    .setMaxParallelism(params.getInt("mp4", 8))
                    .slotSharingGroup("g4");
            leng1.keyBy(0).map(new DumbSink(params.getLong("op5Delay", 100), params.getInt("op5KeyStateSize", 1), params.getBoolean("outputGroundTruth", true)))
                    .disableChaining()
                    .name("FlatMap 5")
                    .uid("op5")
                    .setParallelism(params.getInt("p5", 1))
                    .setMaxParallelism(params.getInt("mp5", 8))
                    .slotSharingGroup("g5");
            env.execute();
            return ;
        }else if(GRAPH_TYPE.equals("1split2join1")){
            SingleOutputStreamOperator<Tuple3<String, Long, Long>> split1 = leng1.keyBy(0)
                    .flatMap(new DumbStatefulMap(params.getLong("op3Delay", 100), params.getInt("op3IoRate", 1), params.getBoolean("op3IoFix", true), params.getInt("op3KeyStateSize", 1)))
                    .disableChaining()
                    .name("FlatMap 3")
                    .uid("op3")
                    .setParallelism(params.getInt("p3", 1))
                    .setMaxParallelism(params.getInt("mp3", 8))
                    .slotSharingGroup("g3");
            SingleOutputStreamOperator<Tuple3<String, Long, Long>> split2 = leng1.keyBy(0)
                    .flatMap(new DumbStatefulMap(params.getLong("op4Delay", 100), params.getInt("op4IoRate", 1), params.getBoolean("op4IoFix", true), params.getInt("op4KeyStateSize", 1)))
                    .disableChaining()
                    .name("FlatMap 4")
                    .uid("op4")
                    .setParallelism(params.getInt("p4", 1))
                    .setMaxParallelism(params.getInt("mp4", 8))
                    .slotSharingGroup("g4");
            DataStream<Tuple3<String, Long, Long>> unionStream =  split1.union(split2);
            unionStream.keyBy(0)
                    .map(new DumbSink(params.getLong("op5Delay", 100), params.getInt("op5KeyStateSize", 1), params.getBoolean("outputGroundTruth", true)))
                    .disableChaining()
                    .name("FlatMap 5")
                    .uid("op5")
                    .setParallelism(params.getInt("p5", 1))
                    .setMaxParallelism(params.getInt("mp5", 8))
                    .slotSharingGroup("g5");
            env.execute();
            return ;
        }

        SingleOutputStreamOperator<Tuple3<String, Long, Long>> leng2 = leng1.keyBy(0)
                .flatMap(new DumbStatefulMap(params.getLong("op3Delay", 100), params.getInt("op3IoRate", 1), params.getBoolean("op3IoFix", true), params.getInt("op3KeyStateSize", 1)))
                .disableChaining()
                .name("FlatMap 3")
                .uid("op3")
                .setParallelism(params.getInt("p3", 1))
                .setMaxParallelism(params.getInt("mp3", 8))
                .slotSharingGroup("g3");

        if(GRAPH_TYPE.equals("3op")){
            leng2.keyBy(0).map(new DumbSink(params.getLong("op4Delay", 100), params.getInt("op4KeyStateSize", 1), params.getBoolean("outputGroundTruth", true)))
                    .disableChaining()
                    .name("FlatMap 4")
                    .uid("op4")
                    .setParallelism(params.getInt("p4", 1))
                    .setMaxParallelism(params.getInt("mp4", 8))
                    .slotSharingGroup("g4");
            env.execute();
            return ;
        }else if(GRAPH_TYPE.equals("2split2")){
            leng2.keyBy(0).map(new DumbSink(params.getLong("op4Delay", 100), params.getInt("op4KeyStateSize", 1), params.getBoolean("outputGroundTruth", true)))
                    .disableChaining()
                    .name("FlatMap 4")
                    .uid("op4")
                    .setParallelism(params.getInt("p4", 1))
                    .setMaxParallelism(params.getInt("mp4", 8))
                    .slotSharingGroup("g4");
            leng2.keyBy(0).map(new DumbSink(params.getLong("op5Delay", 100), params.getInt("op5KeyStateSize", 1), params.getBoolean("outputGroundTruth", true)))
                    .disableChaining()
                    .name("FlatMap 5")
                    .uid("op5")
                    .setParallelism(params.getInt("p5", 1))
                    .setMaxParallelism(params.getInt("mp5", 8))
                    .slotSharingGroup("g5");
            env.execute();
            return ;
        }
        System.out.println("ERROR: Cannot find the specificed graph type: " + GRAPH_TYPE);
    }

    public static final class DumbStatefulMap extends RichFlatMapFunction<Tuple3<String, Long, Long>, Tuple3<String, Long, Long>> {
        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private transient MapState<String, String> countMap;
        private int ioRatio;
        private boolean ioFixedFlag;
        private Random rand;
        private final int perKeyStateSize;
        private long averageDelay = 1000; // micro second

        private final String payload;

        public DumbStatefulMap(long averageDelay, int ioRatio, boolean ioFixedFlag, int perKeyStateSize) {
            this.averageDelay = averageDelay;
            this.ioRatio = ioRatio;
            this.ioFixedFlag = ioFixedFlag;
            this.perKeyStateSize = perKeyStateSize;
            this.payload = StringUtils.repeat("A", perKeyStateSize);
            if(!ioFixedFlag){
                rand = new Random();
            }
        }

        @Override
        public void flatMap(Tuple3<String, Long, Long> input, Collector<Tuple3<String, Long, Long>> out) throws Exception {
            String s = input.f0;
            countMap.put(s, payload);
            if(ioFixedFlag) {
                for (int i = 0; i < ioRatio; i++) {
                    long t = input.f1;
                    long id = input.f2;
                    out.collect(new Tuple3<String, Long, Long>(s, t, id));
                }
            }else{
                int numberOfOutput = rand.nextInt(ioRatio+1);
                for (int i = 0; i < numberOfOutput; i++){
                    long t = input.f1;
                    long id = input.f2;
                    out.collect(new Tuple3<String, Long, Long>(s, t, id));
                }
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
        private final boolean isOutput;

        private int processNumber = 0;
        DumbSink(long averageDelay, int perKeyStateSize, boolean isOutput){
            this.averageDelay = averageDelay;
            this.perKeyStateSize = perKeyStateSize;
            this.payload = StringUtils.repeat("A", perKeyStateSize);
            this.isOutput = isOutput;
        }

        @Override
        public Tuple4<String, Long, Long, Long> map(Tuple3<String, Long, Long> input) throws Exception {
            countMap.put(input.f0, payload);
            processNumber++;
            delay(averageDelay);
            long currentTime = System.currentTimeMillis();
            if(isOutput || processNumber >= 10000) {
                processNumber = 0;
                System.out.println("GT: " + input.f0 + ", " + currentTime + ", " + (currentTime - input.f1) + ", " + input.f2);
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


    public static final class DynamicAvgRateSineSource implements SourceFunction<Tuple3<String, Long, Long>>, CheckpointedFunction {
        private long PHASE1_TIME, PHASE2_TIME, INTERMEDIATE_TIME, PHASE1_RATE, PHASE2_RATE, INTERMEDIATE_RATE, INTERMEDIATE_RANGE, INTERMEDIATE_PERIOD, INTERVAL, macroAmplitude, macroPeriod;
        private int INTERMEDIATE_DELTA;
        private int count = 0;
        private volatile boolean isRunning = true;

        private transient ListState<Integer> checkpointedCount;

        private int maxParallelism;
        private FastZipfGenerator fastZipfGenerator;
        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private int nKeys;
        private final String curve_type;

        private final Map<Integer, List<String>> keyGroupMapping = new HashMap<>();

        private final Map<Integer, Long> totalOutputNumbers = new HashMap<>();

        public DynamicAvgRateSineSource(long PHASE1_TIME, long PHASE2_TIME, long INTERMEDIATE_TIME, long PHASE1_RATE, long PHASE2_RATE, long INTERMEDIATE_RATE, long INTERMEDIATE_RANGE, long INTERMEDIATE_PERIOD, long macro_amplitude, long macro_period, int maxParallelism, double zipfSkew, int nkeys, String curve_type, int delta){
            this.PHASE1_TIME = PHASE1_TIME;
            this.PHASE2_TIME = PHASE2_TIME;
            this.INTERMEDIATE_TIME = INTERMEDIATE_TIME;
            this.PHASE1_RATE = PHASE1_RATE;
            this.PHASE2_RATE = PHASE2_RATE;
            this.INTERMEDIATE_RATE = INTERMEDIATE_RATE;
            this.INTERMEDIATE_RANGE = INTERMEDIATE_RANGE;
            this.INTERMEDIATE_PERIOD = INTERMEDIATE_PERIOD;
            this.INTERMEDIATE_DELTA = delta;
            this.macroAmplitude = macro_amplitude;
            this.macroPeriod = macro_period;
            this.INTERVAL = 50;
            this.nKeys = nkeys;
            this.maxParallelism = maxParallelism;
            this.fastZipfGenerator = new FastZipfGenerator(maxParallelism, zipfSkew, 0, 114514);
            this.curve_type = curve_type;
            for (int i = 0; i < nkeys; i++) {
                String key = "A" + i;
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

        public void startInterPhase(long startTime, SourceContext<Tuple3<String, Long, Long>> ctx, String curve_type, long PHASE1_RATE, long INTERMEDIATE_RATE, long INTERMEDIATE_PERIOD, long INTERMEDIATE_TIME, long macroAmplitude, long macroPeriod, int delta) throws Exception {
            long remainedNumber = (long) Math.floor(PHASE1_RATE * INTERVAL / 1000.0);
            long AMPLITUDE = INTERMEDIATE_RANGE;
            while (isRunning && System.currentTimeMillis() - startTime < INTERMEDIATE_TIME) {
                if (remainedNumber <= 0) {
                    long index = (System.currentTimeMillis() - startTime) / INTERVAL;
                    long ntime = (index + 1) * INTERVAL + startTime;
                    double macroTheta = Math.sin(Math.toRadians(index * INTERVAL * 360 / ((double) macroPeriod)));
                    AMPLITUDE = (INTERMEDIATE_RANGE) + (long) Math.floor(macroTheta * macroAmplitude);
                    double theta = 0.0;
                    if (curve_type.equals("sine")) {
                        theta = Math.sin(Math.toRadians(index * INTERVAL * 360 / ((double) INTERMEDIATE_PERIOD) + delta));
                    } else if (curve_type.equals("gradient")) {
                        theta = 1.0;
                    } else if (curve_type.equals("linear")) {
                        double x = index * INTERVAL - Math.floor(index * INTERVAL / ((double) INTERMEDIATE_PERIOD)) * INTERMEDIATE_PERIOD;
                        if (x >= INTERMEDIATE_PERIOD / 2.0) {
                            theta = 2.0 - x / (INTERMEDIATE_PERIOD / 2.0);
                        } else {
                            theta = x / (INTERMEDIATE_PERIOD / 2.0);
                        }
                        theta = theta * 2 - 1.0;
                    }
                    remainedNumber = (long) Math.floor((INTERMEDIATE_RATE + theta * AMPLITUDE) / 1000 * INTERVAL);
                    long ctime = System.currentTimeMillis();
                    if (ntime >= ctime) {
                        Thread.sleep(ntime - ctime);
                    }
                }
                synchronized (ctx.getCheckpointLock()) {
                    int selectedKeygroup = fastZipfGenerator.next();
                    List<String> subKeySet = keyGroupMapping.get(selectedKeygroup);
                    totalOutputNumbers.put(selectedKeygroup, totalOutputNumbers.getOrDefault(selectedKeygroup, 0l) + 1);
                    String key = getSubKeySetChar(count, subKeySet);
                    ctx.collect(Tuple3.of(key, System.currentTimeMillis(), (long) count));
                    remainedNumber--;
                    count++;
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
                    totalOutputNumbers.put(selectedKeygroup, totalOutputNumbers.getOrDefault(selectedKeygroup, 0l)+1);
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
            startInterPhase(startTime, ctx, curve_type, PHASE1_RATE, INTERMEDIATE_RATE, INTERMEDIATE_PERIOD, INTERMEDIATE_TIME, macroAmplitude, macroPeriod, INTERMEDIATE_DELTA);

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
                    totalOutputNumbers.put(selectedKeygroup, totalOutputNumbers.getOrDefault(selectedKeygroup, 0l)+1);
                    String key = getSubKeySetChar(count, subKeySet);
                    ctx.collect(Tuple3.of(key, System.currentTimeMillis(), (long)count));
                    count++;
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
    public static final class WhenSource implements SourceFunction<Tuple3<String, Long, Long>>, CheckpointedFunction {
        private long NORMAL_TIME, NORMAL_RATE, PHASE1_TIME, PHASE1_RATE, PHASE2_TIME, PHASE2_RATE, TOTAL_TIME;
        private int count = 0;
        private volatile boolean isRunning = true;
        private transient ListState<Integer> checkpointedCount;
        private int maxParallelism;
        private FastZipfGenerator fastZipfGenerator;
        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private int nKeys;
        private final Map<Integer, List<String>> keyGroupMapping = new HashMap<>();

        private final Map<Integer, Long> totalOutputNumbers = new HashMap<>();

        public WhenSource(long PHASE1_TIME, long PHASE2_TIME, long NORMAL_TIME, long PHASE1_RATE, long PHASE2_RATE, long NORMAL_RATE, long runtime){
            this.PHASE1_TIME = PHASE1_TIME;
            this.PHASE2_TIME = PHASE2_TIME;
            this.NORMAL_TIME = NORMAL_TIME;
            this.PHASE1_RATE = PHASE1_RATE;
            this.PHASE2_RATE = PHASE2_RATE;
            this.NORMAL_RATE = NORMAL_RATE;
            this.TOTAL_TIME = runtime;
            this.nKeys = 1000;
            this.maxParallelism = 128;
            this.fastZipfGenerator = new FastZipfGenerator(maxParallelism, 0.0, 0, 114514);
            for (int i = 0; i < this.nKeys; i++) {
                String key = "A" + i;
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
        void startPhase(SourceContext<Tuple3<String, Long, Long>> ctx, long rate, long time, long phaseStartTime)throws Exception {
            while (isRunning && System.currentTimeMillis() - phaseStartTime < time) {
                long emitStartTime = System.currentTimeMillis();
                for (int i = 0; i < rate / 20; i++) {
                    int selectedKeygroup = fastZipfGenerator.next();
                    List<String> subKeySet = keyGroupMapping.get(selectedKeygroup);
                    totalOutputNumbers.put(selectedKeygroup, totalOutputNumbers.getOrDefault(selectedKeygroup, 0l) + 1);
                    String key = getSubKeySetChar(count, subKeySet);
                    ctx.collect(Tuple3.of(key, System.currentTimeMillis(), (long) count));
                    count++;
                }
                Util.pause(emitStartTime);
            }
        }

        public void run(SourceContext<Tuple3<String, Long, Long>> ctx) throws Exception {
            long startTime = System.currentTimeMillis();
            System.out.println("Source start at: " + startTime);
            System.out.println("Source warm up...");
            startPhase(ctx, NORMAL_RATE, 20 * 1000, startTime);
            startTime = System.currentTimeMillis();
            long round = 0;
            while (isRunning && System.currentTimeMillis() - startTime < TOTAL_TIME) {
                round++;
                long roundStartTime = System.currentTimeMillis();
                System.out.println("Round " + round + " normal phase start at: " + roundStartTime);
                startPhase(ctx, NORMAL_RATE, NORMAL_TIME, roundStartTime);

                if (!isRunning) {
                    return;
                }

                roundStartTime = System.currentTimeMillis();
                System.out.println("Round " + round + " phase 1 start at: " + roundStartTime);
                startPhase(ctx, PHASE1_RATE, PHASE1_TIME, roundStartTime);
                if (!isRunning) {
                    return;
                }

                roundStartTime = System.currentTimeMillis();
                System.out.println("Round " + round + " normal phase start at: " + roundStartTime);
                startPhase(ctx, NORMAL_RATE, NORMAL_TIME, roundStartTime);
                if (!isRunning) {
                    return;
                }

                roundStartTime = System.currentTimeMillis();
                System.out.println("Round " + round + " phase 2 start at: " + roundStartTime);
                startPhase(ctx, PHASE2_RATE, PHASE2_TIME, roundStartTime);
                if (!isRunning) {
                    return;
                }
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
    public static final class HowSource implements SourceFunction<Tuple3<String, Long, Long>>, CheckpointedFunction {
        private long NORMAL_TIME, NORMAL_RATE, PHASE1_TIME, PHASE1_RATE, PHASE2_TIME, PHASE2_RATE, TOTAL_TIME;
        private int count = 0;
        private volatile boolean isRunning = true;
        private transient ListState<Integer> checkpointedCount;
        private int maxParallelism;
        private FastZipfGenerator fastZipfGenerator;
        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private int nKeys;
        private final Map<Integer, List<String>> keyGroupMapping = new HashMap<>();

        private final Map<Integer, Long> totalOutputNumbers = new HashMap<>();

        public HowSource(long PHASE1_TIME, long PHASE2_TIME, long NORMAL_TIME, long PHASE1_RATE, long PHASE2_RATE, long NORMAL_RATE, long runtime){
            this.PHASE1_TIME = PHASE1_TIME;
            this.PHASE2_TIME = PHASE2_TIME;
            this.NORMAL_TIME = NORMAL_TIME;
            this.PHASE1_RATE = PHASE1_RATE;
            this.PHASE2_RATE = PHASE2_RATE;
            this.NORMAL_RATE = NORMAL_RATE;
            this.TOTAL_TIME = runtime;
            this.nKeys = 1000;
            this.maxParallelism = 128;
            this.fastZipfGenerator = new FastZipfGenerator(maxParallelism, 0.0, 0, 114514);
            for (int i = 0; i < this.nKeys; i++) {
                String key = "A" + i;
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
        void startPhase(SourceContext<Tuple3<String, Long, Long>> ctx, long rate, long time, long phaseStartTime)throws Exception {
            while (isRunning && System.currentTimeMillis() - phaseStartTime < time) {
                long emitStartTime = System.currentTimeMillis();
                for (int i = 0; i < rate / 20; i++) {
                    int selectedKeygroup = fastZipfGenerator.next();
                    List<String> subKeySet = keyGroupMapping.get(selectedKeygroup);
                    totalOutputNumbers.put(selectedKeygroup, totalOutputNumbers.getOrDefault(selectedKeygroup, 0l) + 1);
                    String key = getSubKeySetChar(count, subKeySet);
                    ctx.collect(Tuple3.of(key, System.currentTimeMillis(), (long) count));
                    count++;
                }
                Util.pause(emitStartTime);
            }
        }

        void startLinearPhase(SourceContext<Tuple3<String, Long, Long>> ctx, long rate, long time, long phaseStartTime, long normalRate)throws Exception {
            long remainedNumber = (long) Math.floor(PHASE1_RATE * 50 / 1000.0);
            while (isRunning && System.currentTimeMillis() - phaseStartTime < time) {
                if(remainedNumber <= 0) {
                    long index = (System.currentTimeMillis() - phaseStartTime) / 50;
                    long ntime = (index + 1) * 50 + phaseStartTime;
                    if(index < time / 2 / 50 ){
                        remainedNumber = (long) Math.floor((index * (rate - normalRate) / (time / 2.0 / 50.0) + normalRate) / 1000.0 * 50);
                    }else {
                        remainedNumber = (long) Math.floor((rate - (index - time / 2.0 / 50.0) * (rate - normalRate) / (time / 2.0 / 50.0)) / 1000.0 * 50);
                    }
                    long ctime = System.currentTimeMillis();
                    if (ntime >= ctime) {
                        Thread.sleep(ntime - ctime);
                    }
                }
                synchronized (ctx.getCheckpointLock()){
                    int selectedKeygroup = fastZipfGenerator.next();
                    List<String> subKeySet = keyGroupMapping.get(selectedKeygroup);
                    totalOutputNumbers.put(selectedKeygroup, totalOutputNumbers.getOrDefault(selectedKeygroup, 0l) + 1);
                    String key = getSubKeySetChar(count, subKeySet);
                    ctx.collect(Tuple3.of(key, System.currentTimeMillis(), (long) count));
                    remainedNumber --;
                    count++;
                }
            }
        }

        void startSinePhase(SourceContext<Tuple3<String, Long, Long>> ctx, long rate, long time, long phaseStartTime, long normalRate)throws Exception {
            long remainedNumber = (long) Math.floor(PHASE1_RATE * 50 / 1000.0);
            while (isRunning && System.currentTimeMillis() - phaseStartTime < time) {
                if(remainedNumber <= 0) {
                    long index = (System.currentTimeMillis() - phaseStartTime) / 50;
                    long ntime = (index + 1) * 50 + phaseStartTime;
                    double delta = Math.sin(Math.toRadians(index * 50 * 360.0 / (time * 2)));
                    remainedNumber = (long) Math.floor((delta * (rate - normalRate) + normalRate) / 1000.0 * 50);
                    long ctime = System.currentTimeMillis();
                    if (ntime >= ctime) {
                        Thread.sleep(ntime - ctime);
                    }
                }
                synchronized (ctx.getCheckpointLock()){
                    int selectedKeygroup = fastZipfGenerator.next();
                    List<String> subKeySet = keyGroupMapping.get(selectedKeygroup);
                    totalOutputNumbers.put(selectedKeygroup, totalOutputNumbers.getOrDefault(selectedKeygroup, 0l) + 1);
                    String key = getSubKeySetChar(count, subKeySet);
                    ctx.collect(Tuple3.of(key, System.currentTimeMillis(), (long) count));
                    remainedNumber --;
                    count++;
                }
            }
        }


        public void run(SourceContext<Tuple3<String, Long, Long>> ctx) throws Exception {
            long startTime = System.currentTimeMillis();
            System.out.println("Source start at: " + startTime);
            System.out.println("Source warm up...");
            startPhase(ctx, NORMAL_RATE, 20 * 1000, startTime);
            startTime = System.currentTimeMillis();
            long round = 0;
            while (isRunning && System.currentTimeMillis() - startTime < TOTAL_TIME) {
                round++;
                long roundStartTime = System.currentTimeMillis();
                System.out.println("Round " + round + " normal phase start at: " + roundStartTime);
                startPhase(ctx, NORMAL_RATE, NORMAL_TIME, roundStartTime);

                if (!isRunning) {
                    return;
                }

                roundStartTime = System.currentTimeMillis();
                System.out.println("Round " + round + " phase 1 start at: " + roundStartTime);
                if(round % 3 == 1){
                    startPhase(ctx, PHASE1_RATE, PHASE1_TIME, roundStartTime);
                }else if(round % 3 == 2){
                    startLinearPhase(ctx, PHASE1_RATE, PHASE1_TIME, roundStartTime, NORMAL_RATE);
                }else {
                    startSinePhase(ctx, PHASE1_RATE, PHASE1_TIME, roundStartTime, NORMAL_RATE);
                }

                if (!isRunning) {
                    return;
                }

                roundStartTime = System.currentTimeMillis();
                System.out.println("Round " + round + " normal phase start at: " + roundStartTime);
                startPhase(ctx, NORMAL_RATE, NORMAL_TIME, roundStartTime);
                if (!isRunning) {
                    return;
                }

                roundStartTime = System.currentTimeMillis();
                System.out.println("Round " + round + " phase 2 start at: " + roundStartTime);
                if(round % 3 == 1){
                    startPhase(ctx, PHASE2_RATE, PHASE2_TIME, roundStartTime);
                }else if(round % 3 == 2){
                    startLinearPhase(ctx, PHASE2_RATE, PHASE2_TIME, roundStartTime, NORMAL_RATE);
                }else {
                    startSinePhase(ctx, PHASE2_RATE, PHASE2_TIME, roundStartTime, NORMAL_RATE);
                }
                if (!isRunning) {
                    return;
                }
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
