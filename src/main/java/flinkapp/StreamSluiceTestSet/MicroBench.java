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
        if(SOURCE_TYPE.equals("changing_amplitude_and_period")){
            source = env.addSource(new ChangingAmplitudeAndPeriodSource(params.getLong("warmupTime", 20) * 1000, PHASE1_TIME, PHASE2_TIME, INTERMEDIATE_TIME, params.getLong("warmupRate", INTERMEDIATE_RATE), PHASE1_RATE, PHASE2_RATE, INTERMEDIATE_RATE, params.getLong("run_time", 510) * 1000, params.get("curve_type", "sine"))).
                    setParallelism(params.getInt("p1", 1));
        }else if(SOURCE_TYPE.equals("sine_two_phase")) {
            source = env.addSource(new SineTwoPhase(params.getLong("warmupTime", 20) * 1000, PHASE1_TIME, PHASE2_TIME, INTERMEDIATE_TIME, params.getLong("warmupRate", INTERMEDIATE_RATE), PHASE1_RATE, PHASE2_RATE, INTERMEDIATE_RATE, params.getLong("run_time", 510) * 1000, params.get("curve_type", "sine"))).
                    setParallelism(params.getInt("p1", 1));
        }else if(SOURCE_TYPE.equals("linear_phase_change")){
            source = env.addSource(new LinearChangingAveragePhase(params.getLong("warmupTime", 20) * 1000, PHASE1_TIME, PHASE2_TIME, INTERMEDIATE_TIME, params.getLong("warmupRate", INTERMEDIATE_RATE), PHASE1_RATE, PHASE2_RATE, INTERMEDIATE_RATE, params.getLong("run_time", 510) * 1000, params.get("curve_type", "sine"))).
                    setParallelism(params.getInt("p1", 1));
        }else if(SOURCE_TYPE.equals("sine_shift")){
            source = env.addSource(new SinePhaseShift(params.getLong("warmupTime", 20) * 1000, PHASE1_TIME, PHASE2_TIME, INTERMEDIATE_TIME, params.getLong("warmupRate", INTERMEDIATE_RATE), PHASE1_RATE, PHASE2_RATE, INTERMEDIATE_RATE, params.getLong("run_time", 510) * 1000, params.get("curve_type", "sine"))).
                    setParallelism(params.getInt("p1", 1));
        }else if(SOURCE_TYPE.equals("sine_with_spike")){
            source = env.addSource(new SineWithSpikeSource(params.getLong("warmupTime", 20) * 1000, PHASE1_TIME, PHASE2_TIME, INTERMEDIATE_TIME, params.getLong("warmupRate", INTERMEDIATE_RATE), PHASE1_RATE, PHASE2_RATE, INTERMEDIATE_RATE, params.getLong("run_time", 510) * 1000, params.get("curve_type", "sine"))).
                    setParallelism(params.getInt("p1", 1));
        }else if(SOURCE_TYPE.equals("changing_period")){
            source = env.addSource(new ChangingPeriodSource(params.getLong("warmupTime", 20) * 1000, PHASE1_TIME, PHASE2_TIME, INTERMEDIATE_TIME, params.getLong("warmupRate", INTERMEDIATE_RATE), PHASE1_RATE, PHASE2_RATE, INTERMEDIATE_RATE, params.getLong("run_time", 510) * 1000, params.get("curve_type", "sine"))).
                    setParallelism(params.getInt("p1", 1));
        }else if(SOURCE_TYPE.equals("changing_amplitude")) {
            source = env.addSource(new ChangingAmplitudeSource(params.getLong("warmupTime", 20) * 1000, PHASE1_TIME, PHASE2_TIME, INTERMEDIATE_TIME, params.getLong("warmupRate", INTERMEDIATE_RATE), PHASE1_RATE, PHASE2_RATE, INTERMEDIATE_RATE, params.getLong("run_time", 510) * 1000, params.get("curve_type", "sine"))).
                    setParallelism(params.getInt("p1", 1));
        }else if(SOURCE_TYPE.equals("when")){
            source = env.addSource(new WhenSource(params.getLong("warmupTime", 20) * 1000, PHASE1_TIME, PHASE2_TIME, INTERMEDIATE_TIME, params.getLong("warmupRate", INTERMEDIATE_RATE), PHASE1_RATE, PHASE2_RATE, INTERMEDIATE_RATE, params.getLong("run_time", 510) * 1000, params.get("curve_type", "sine"))).
                    setParallelism(params.getInt("p1", 1));
        }else if(SOURCE_TYPE.equals("how")) {
            source = env.addSource(new HowSource(PHASE1_TIME, PHASE2_TIME, INTERMEDIATE_TIME, PHASE1_RATE, PHASE2_RATE, INTERMEDIATE_RATE, params.getLong("run_time", 510) * 1000));
        }else {
            source = env.addSource(new DynamicAvgRateSineSource(PHASE1_TIME, PHASE2_TIME, INTERMEDIATE_TIME, PHASE1_RATE, PHASE2_RATE, INTERMEDIATE_RATE, INTERMEDIATE_RANGE, INTERMEDIATE_PERIOD, params.getLong("macroInterAmplitude", 0), params.getLong("macroInterPeriod", 60) * 1000, params.getInt("mp2", 8), zipf_skew, nKeys, params.get("curve_type", "sine"), params.getInt("inter_delta", 0)))
                    .setParallelism(params.getInt("p1", 1));
        }
        if(GRAPH_TYPE.endsWith("op_line")){
            int op_n = Integer.parseInt(GRAPH_TYPE.substring(0, GRAPH_TYPE.length() - 7));
            SingleOutputStreamOperator<Tuple3<String, Long, Long>> leng_t = source;
            for(int i = 1; i < op_n; i++){
                leng_t = leng_t.keyBy(0)
                        .flatMap(new DumbStatefulMap(params.getLong("op4Delay", 100), params.getInt("op4IoRate", 1), params.getBoolean("op4IoFix", true), params.getInt("op4KeyStateSize", 1)))
                        .disableChaining()
                        .name(String.format("FlatMap %d", i + 1))
                        .uid(String.format("op%d", i + 1))
                        .setParallelism(params.getInt("p4", 1))
                        .setMaxParallelism(params.getInt("mp4", 8))
                        .slotSharingGroup(String.format("g%d", i + 1));
            }
            leng_t.keyBy(0).map(new DumbSink(params.getLong("op5Delay", 100), params.getInt("op5KeyStateSize", 1), params.getBoolean("outputGroundTruth", true)))
                    .disableChaining()
                    .name(String.format("FlatMap %d", op_n + 1))
                    .uid(String.format("op%d", op_n + 1))
                    .setParallelism(params.getInt("p5", 1))
                    .setMaxParallelism(params.getInt("mp5", 8))
                    .slotSharingGroup(String.format("g%d", op_n + 1));
            env.execute();
            return ;
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
        private final boolean isOutput;
        private Random rand;
        private final int perKeyStateSize;
        private final long averageDelay; // micro second
        private final String opName;

        private final String payload;

        public DumbStatefulMap(long averageDelay, int ioRatio, boolean ioFixedFlag, int perKeyStateSize) {
            this.averageDelay = averageDelay;
            this.ioRatio = ioRatio;
            this.ioFixedFlag = ioFixedFlag;
            this.perKeyStateSize = perKeyStateSize;
            this.payload = StringUtils.repeat("A", perKeyStateSize);
            this.isOutput = false;
            this.opName = "";
            if(!ioFixedFlag){
                rand = new Random();
            }
        }
        public DumbStatefulMap(long averageDelay, int ioRatio, boolean ioFixedFlag, int perKeyStateSize, boolean isOutput, String opName) {
            this.averageDelay = averageDelay;
            this.ioRatio = ioRatio;
            this.ioFixedFlag = ioFixedFlag;
            this.perKeyStateSize = perKeyStateSize;
            this.payload = StringUtils.repeat("A", perKeyStateSize);
            this.isOutput = isOutput;
            this.opName = opName;
            if(!ioFixedFlag){
                rand = new Random();
            }
        }

        @Override
        public void flatMap(Tuple3<String, Long, Long> input, Collector<Tuple3<String, Long, Long>> out) throws Exception {
            if(isOutput) {
                long currentTime = System.currentTimeMillis();
                System.out.println("OP_IN " + this.opName + ": " + input.f0 + ", " + currentTime + ", " + (currentTime - input.f1) + ", " + input.f2);
            }
            String s = input.f0;
            countMap.put(s, payload);
            delay(averageDelay);
            if(ioFixedFlag) {
                for (int i = 0; i < ioRatio; i++) {
                    long t = input.f1;
                    long id = input.f2;
                    out.collect(new Tuple3<String, Long, Long>(s, t, id));
                    if(isOutput){
                        long currentTime = System.currentTimeMillis();
                        System.out.println("OP_OUT " + this.opName + ": " + input.f0 + ", " + currentTime + ", " + (currentTime - input.f1) + ", " + input.f2);
                    }
                }
            }else{
                int numberOfOutput = rand.nextInt(ioRatio+1);
                for (int i = 0; i < numberOfOutput; i++){
                    long t = input.f1;
                    long id = input.f2;
                    out.collect(new Tuple3<String, Long, Long>(s, t, id));
                }
            }
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
        final private boolean withStairFlag;
        final private long WARMP_TIME, WARMP_RATE, NORMAL_TIME, NORMAL_RATE, PHASE1_TIME, PHASE1_RATE, PHASE2_TIME, PHASE2_RATE, TOTAL_TIME;
        private int count = 0;
        private final int curve_type;
        private volatile boolean isRunning = true;
        private transient ListState<Integer> checkpointedCount;
        private int maxParallelism;
        private FastZipfGenerator fastZipfGenerator;
        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private int nKeys;
        private final Map<Integer, List<String>> keyGroupMapping = new HashMap<>();

        private final Map<Integer, Long> totalOutputNumbers = new HashMap<>();

        public WhenSource(long WARMP_TIME, long PHASE1_TIME, long PHASE2_TIME, long NORMAL_TIME, long WARMP_RATE, long PHASE1_RATE, long PHASE2_RATE, long NORMAL_RATE, long runtime, String curve_type){
            if(NORMAL_TIME == 0){
                withStairFlag = false;
            }else {
                withStairFlag = true;
            }
            this.WARMP_TIME = WARMP_TIME;
            this.WARMP_RATE = WARMP_RATE;
            this.PHASE1_TIME = PHASE1_TIME;
            this.PHASE2_TIME = PHASE2_TIME;
            this.NORMAL_TIME = NORMAL_TIME;
            this.PHASE1_RATE = PHASE1_RATE;
            this.PHASE2_RATE = PHASE2_RATE;
            this.NORMAL_RATE = NORMAL_RATE;
            this.TOTAL_TIME = runtime;
            if(curve_type.equals("gradient")) {
                this.curve_type = 0;
            }else if(curve_type.equals("linear")){
                this.curve_type = 1;
            }else if(curve_type.equals("quarter-sine")) {
                this.curve_type = 2;
            }else if(curve_type.equals("sine")){
                this.curve_type = 3;
            }else if(curve_type.equals("mixed")) {
                this.curve_type = 4;
            }else{
                this.curve_type = 0;
            }
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
        void startSteadyPhase(SourceContext<Tuple3<String, Long, Long>> ctx, long rate, long time, long phaseStartTime)throws Exception {
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
        void startLinearPhase(SourceContext<Tuple3<String, Long, Long>> ctx, long rate1, long rate2, long time, long phaseStartTime)throws Exception {
            long currentTime;
            long elapsedTime;
            long rate;

            while (isRunning && (currentTime = System.currentTimeMillis()) - phaseStartTime < time) {
                long emitStartTime = System.currentTimeMillis();
                elapsedTime = currentTime - phaseStartTime;

                // Calculate the current rate using linear interpolation between rate1 and rate2
                rate = rate1 + ((rate2 - rate1) * elapsedTime) / time;


                for (int i = 0; i < rate / 20; i++) {
                    int selectedKeygroup = fastZipfGenerator.next();
                    List<String> subKeySet = keyGroupMapping.get(selectedKeygroup);
                    totalOutputNumbers.put(selectedKeygroup, totalOutputNumbers.getOrDefault(selectedKeygroup, 0L) + 1);
                    String key = getSubKeySetChar(count, subKeySet);
                    ctx.collect(Tuple3.of(key, System.currentTimeMillis(), (long) count));
                    count++;
                }

                Util.pause(emitStartTime);
            }
        }

        void startQuarterSinePhase(SourceContext<Tuple3<String, Long, Long>> ctx, long rate1, long rate2, long time, long phaseStartTime) throws Exception {
            long currentTime;
            long elapsedTime;
            long rate;

            while (isRunning && (currentTime = System.currentTimeMillis()) - phaseStartTime < time) {
                long emitStartTime = System.currentTimeMillis();
                elapsedTime = currentTime - phaseStartTime;

                // Calculate the rate using the first 90 degrees of a sine function
                double sineValue = Math.sin((Math.PI / 2) * ((double) elapsedTime / time));
                rate = (long) (rate1 + (rate2 - rate1) * sineValue);


                for (int i = 0; i < rate / 20; i++) {
                    int selectedKeygroup = fastZipfGenerator.next();
                    List<String> subKeySet = keyGroupMapping.get(selectedKeygroup);
                    totalOutputNumbers.put(selectedKeygroup, totalOutputNumbers.getOrDefault(selectedKeygroup, 0L) + 1);
                    String key = getSubKeySetChar(count, subKeySet);
                    ctx.collect(Tuple3.of(key, System.currentTimeMillis(), (long) count));
                    count++;
                }

                Util.pause(emitStartTime);
            }
        }

        void startSinePhase(SourceContext<Tuple3<String, Long, Long>> ctx, long amplitude, long baseRate, long time, long phaseStartTime) throws Exception {
            long currentTime;
            long elapsedTime;
            long rate;

            while (isRunning && (currentTime = System.currentTimeMillis()) - phaseStartTime < time) {
                long emitStartTime = System.currentTimeMillis();
                elapsedTime = currentTime - phaseStartTime;

                // Calculate the rate using a sine function
                rate = (long) (baseRate + amplitude * Math.sin((2 * Math.PI * elapsedTime) / time));


                for (int i = 0; i < rate / 20; i++) {
                    int selectedKeygroup = fastZipfGenerator.next();
                    List<String> subKeySet = keyGroupMapping.get(selectedKeygroup);
                    totalOutputNumbers.put(selectedKeygroup, totalOutputNumbers.getOrDefault(selectedKeygroup, 0L) + 1);
                    String key = getSubKeySetChar(count, subKeySet);
                    ctx.collect(Tuple3.of(key, System.currentTimeMillis(), (long) count));
                    count++;
                }

                Util.pause(emitStartTime);
            }
        }

        private void generateWithStairCurve(SourceContext<Tuple3<String, Long, Long>> ctx) throws Exception {
            long startTime = System.currentTimeMillis();
            System.out.println("Source start at: " + startTime);
            System.out.println("Source warm up...");
            // startSteadyPhase(ctx, NORMAL_RATE, 20 * 1000, startTime);
            startSteadyPhase(ctx, WARMP_RATE, WARMP_TIME, startTime);
            startTime = System.currentTimeMillis();
            long round = 0;
            while (isRunning && System.currentTimeMillis() - startTime < TOTAL_TIME) {
                round++;
                int this_round_curve;
                if(curve_type != 4){
                    this_round_curve = curve_type;
                }else{
                    this_round_curve = (int)((round - 1) % 4);
                }
                if(this_round_curve == 0) { // Gradient
                    long roundStartTime = System.currentTimeMillis();
                    System.out.println("Round " + round + " normal phase start at: " + roundStartTime);
                    startSteadyPhase(ctx, NORMAL_RATE, NORMAL_TIME / 2, roundStartTime);
                    if (!isRunning) {
                        return;
                    }

                    roundStartTime = System.currentTimeMillis();
                    System.out.println("Round " + round + " phase 1 start at: " + roundStartTime);
                    startSteadyPhase(ctx, PHASE1_RATE, PHASE1_TIME, roundStartTime);

                    if (!isRunning) {
                        return;
                    }

                    roundStartTime = System.currentTimeMillis();
                    System.out.println("Round " + round + " normal phase start at: " + roundStartTime);
                    startSteadyPhase(ctx, NORMAL_RATE, NORMAL_TIME, roundStartTime);
                    if (!isRunning) {
                        return;
                    }

                    roundStartTime = System.currentTimeMillis();
                    System.out.println("Round " + round + " phase 2 start at: " + roundStartTime);
                    startSteadyPhase(ctx, PHASE2_RATE, PHASE2_TIME, roundStartTime);
                    if (!isRunning) {
                        return;
                    }
                    roundStartTime = System.currentTimeMillis();
                    System.out.println("Round " + round + " normal phase start at: " + roundStartTime);
                    startSteadyPhase(ctx, NORMAL_RATE, NORMAL_TIME / 2, roundStartTime);

                    if (!isRunning) {
                        return;
                    }
                }else if(this_round_curve == 1){ // Linear
                    long roundStartTime = System.currentTimeMillis();
                    System.out.println("Round " + round + " normal phase start at: " + roundStartTime);
                    startSteadyPhase(ctx, NORMAL_RATE, NORMAL_TIME / 2, roundStartTime);
                    if (!isRunning) {
                        return;
                    }

                    roundStartTime = System.currentTimeMillis();
                    System.out.println("Round " + round + " phase 1 start at: " + roundStartTime);
                    startLinearPhase(ctx, NORMAL_RATE, PHASE1_RATE, PHASE1_TIME / 2, roundStartTime);
                    roundStartTime = System.currentTimeMillis();
                    startSteadyPhase(ctx, PHASE1_RATE, PHASE1_TIME / 2, roundStartTime);

                    if (!isRunning) {
                        return;
                    }

                    roundStartTime = System.currentTimeMillis();
                    System.out.println("Round " + round + " normal phase start at: " + roundStartTime);
                    startLinearPhase(ctx, PHASE1_RATE, NORMAL_RATE, NORMAL_TIME / 2, roundStartTime);
                    roundStartTime = System.currentTimeMillis();
                    startSteadyPhase(ctx, NORMAL_RATE, NORMAL_TIME / 2, roundStartTime);
                    if (!isRunning) {
                        return;
                    }

                    roundStartTime = System.currentTimeMillis();
                    System.out.println("Round " + round + " phase 2 start at: " + roundStartTime);
                    startLinearPhase(ctx, NORMAL_RATE, PHASE2_RATE, PHASE2_TIME / 2, roundStartTime);
                    roundStartTime = System.currentTimeMillis();
                    startSteadyPhase(ctx, PHASE2_RATE, PHASE2_TIME / 2, roundStartTime);
                    if (!isRunning) {
                        return;
                    }
                    roundStartTime = System.currentTimeMillis();
                    System.out.println("Round " + round + " normal phase start at: " + roundStartTime);
                    startLinearPhase(ctx, PHASE2_RATE, NORMAL_RATE, NORMAL_TIME / 2, roundStartTime);
                    if (!isRunning) {
                        return;
                    }
                }else if(this_round_curve == 2){ // Quarter sine
                    long roundStartTime = System.currentTimeMillis();
                    System.out.println("Round " + round + " normal phase start at: " + roundStartTime);
                    startSteadyPhase(ctx, NORMAL_RATE, NORMAL_TIME / 2, roundStartTime);
                    if (!isRunning) {
                        return;
                    }

                    roundStartTime = System.currentTimeMillis();
                    System.out.println("Round " + round + " phase 1 start at: " + roundStartTime);
                    startQuarterSinePhase(ctx, NORMAL_RATE, PHASE1_RATE, PHASE1_TIME / 2, roundStartTime);
                    roundStartTime = System.currentTimeMillis();
                    startSteadyPhase(ctx, PHASE1_RATE, PHASE1_TIME / 2, roundStartTime);

                    if (!isRunning) {
                        return;
                    }

                    roundStartTime = System.currentTimeMillis();
                    System.out.println("Round " + round + " normal phase start at: " + roundStartTime);
                    startQuarterSinePhase(ctx, PHASE1_RATE, NORMAL_RATE, NORMAL_TIME / 2, roundStartTime);
                    roundStartTime = System.currentTimeMillis();
                    startSteadyPhase(ctx, NORMAL_RATE, NORMAL_TIME / 2, roundStartTime);
                    if (!isRunning) {
                        return;
                    }

                    roundStartTime = System.currentTimeMillis();
                    System.out.println("Round " + round + " phase 2 start at: " + roundStartTime);
                    startQuarterSinePhase(ctx, NORMAL_RATE, PHASE2_RATE, PHASE2_TIME / 2, roundStartTime);
                    roundStartTime = System.currentTimeMillis();
                    startSteadyPhase(ctx, PHASE2_RATE, PHASE2_TIME / 2, roundStartTime);
                    if (!isRunning) {
                        return;
                    }
                    roundStartTime = System.currentTimeMillis();
                    System.out.println("Round " + round + " normal phase start at: " + roundStartTime);
                    startQuarterSinePhase(ctx, PHASE2_RATE, NORMAL_RATE, NORMAL_TIME / 2, roundStartTime);
                    if (!isRunning) {
                        return;
                    }
                }else if(this_round_curve == 3){ // Sine
                    long roundStartTime = System.currentTimeMillis();
                    System.out.println("Round " + round + " normal phase start at: " + roundStartTime);
                    startSteadyPhase(ctx, NORMAL_RATE, NORMAL_TIME / 2, roundStartTime);
                    if (!isRunning) {
                        return;
                    }

                    roundStartTime = System.currentTimeMillis();
                    System.out.println("Round " + round + " phase 1 start at: " + roundStartTime);
                    // startSinePhase(ctx, NORMAL_RATE - PHASE2_RATE,  NORMAL_RATE, NORMAL_TIME + PHASE1_TIME + PHASE2_TIME, roundStartTime);
                    startSinePhase(ctx, PHASE1_RATE - NORMAL_RATE,  NORMAL_RATE, NORMAL_TIME + PHASE1_TIME + PHASE2_TIME, roundStartTime);

                    if (!isRunning) {
                        return;
                    }
                    roundStartTime = System.currentTimeMillis();
                    System.out.println("Round " + round + " normal phase start at: " + roundStartTime);
                    startSteadyPhase(ctx, NORMAL_RATE, NORMAL_TIME / 2, roundStartTime);
                    if (!isRunning) {
                        return;
                    }
                }
            }
        }

        private void generateWithOutStairCurve(SourceContext<Tuple3<String, Long, Long>> ctx) throws Exception {
            long startTime = System.currentTimeMillis();
            System.out.println("Source start at: " + startTime);
            System.out.println("Source warm up...");
            // startSteadyPhase(ctx, NORMAL_RATE, 20 * 1000, startTime);
            startSteadyPhase(ctx, WARMP_RATE, WARMP_TIME, startTime);
            startTime = System.currentTimeMillis();
            long round = 0;
            while (isRunning && System.currentTimeMillis() - startTime < TOTAL_TIME) {
                round++;
                int this_round_curve;
                if(curve_type != 4){
                    this_round_curve = curve_type;
                }else{
                    this_round_curve = (int)((round - 1) % 4);
                }
                if(this_round_curve == 0) { // Gradient
                    long roundStartTime = System.currentTimeMillis();
                    System.out.println("Round " + round + " normal phase start at: " + roundStartTime);
                    startSteadyPhase(ctx, NORMAL_RATE, PHASE1_TIME / 4, roundStartTime);
                    if (!isRunning) {
                        return;
                    }
                    roundStartTime = System.currentTimeMillis();
                    System.out.println("Round " + round + " phase 1 start at: " + roundStartTime);
                    startSteadyPhase(ctx, PHASE1_RATE, PHASE1_TIME / 2, roundStartTime);

                    if (!isRunning) {
                        return;
                    }

                    roundStartTime = System.currentTimeMillis();
                    System.out.println("Round " + round + " normal phase start at: " + roundStartTime);
                    startSteadyPhase(ctx, NORMAL_RATE, PHASE1_TIME / 4 + PHASE2_TIME / 4, roundStartTime);
                    if (!isRunning) {
                        return;
                    }

                    roundStartTime = System.currentTimeMillis();
                    System.out.println("Round " + round + " phase 2 start at: " + roundStartTime);
                    startSteadyPhase(ctx, PHASE2_RATE, PHASE2_TIME / 2, roundStartTime);
                    if (!isRunning) {
                        return;
                    }
                    roundStartTime = System.currentTimeMillis();
                    System.out.println("Round " + round + " normal phase start at: " + roundStartTime);
                    startSteadyPhase(ctx, NORMAL_RATE, PHASE2_TIME / 4, roundStartTime);
                    if (!isRunning) {
                        return;
                    }
                }else if(this_round_curve == 1){ // Linear
                    long roundStartTime = System.currentTimeMillis();
                    System.out.println("Linear Round " + round + " phase 1 start at: " + roundStartTime);
                    startLinearPhase(ctx, NORMAL_RATE, PHASE1_RATE, PHASE1_TIME / 2, roundStartTime);
                    roundStartTime = System.currentTimeMillis();
                    startLinearPhase(ctx, PHASE1_RATE, NORMAL_RATE, PHASE1_TIME / 2, roundStartTime);
                    if (!isRunning) {
                        return;
                    }
                    roundStartTime = System.currentTimeMillis();
                    System.out.println("Round " + round + " phase 2 start at: " + roundStartTime);
                    startLinearPhase(ctx, NORMAL_RATE, PHASE2_RATE, PHASE2_TIME / 2, roundStartTime);
                    roundStartTime = System.currentTimeMillis();
                    startLinearPhase(ctx, PHASE2_RATE, NORMAL_RATE, PHASE2_TIME / 2, roundStartTime);
                    if (!isRunning) {
                        return;
                    }
                }else if(this_round_curve == 2){ // Quarter sine
                    // TODO: add quarter sine
                    long roundStartTime = System.currentTimeMillis();
                    System.out.println("Round " + round + " phase 1 start at: " + roundStartTime);
                    startQuarterSinePhase(ctx, NORMAL_RATE, PHASE1_RATE, PHASE1_TIME / 2, roundStartTime);
                    roundStartTime = System.currentTimeMillis();
                    startQuarterSinePhase(ctx, PHASE1_RATE, NORMAL_RATE, PHASE1_TIME / 2, roundStartTime);
                    if (!isRunning) {
                        return;
                    }
                    roundStartTime = System.currentTimeMillis();
                    System.out.println("Round " + round + " phase 2 start at: " + roundStartTime);
                    startQuarterSinePhase(ctx, NORMAL_RATE, PHASE2_RATE, PHASE2_TIME / 2, roundStartTime);
                    roundStartTime = System.currentTimeMillis();
                    startQuarterSinePhase(ctx, PHASE2_RATE, NORMAL_RATE, PHASE2_TIME / 2, roundStartTime);
                    if (!isRunning) {
                        return;
                    }
                }else if(this_round_curve == 3){ // Sine
                    long roundStartTime = System.currentTimeMillis();
                      System.out.println("Round " + round + " phase 1 start at: " + roundStartTime);
                    // startSinePhase(ctx, NORMAL_RATE - PHASE2_RATE,  NORMAL_RATE, NORMAL_TIME + PHASE1_TIME + PHASE2_TIME, roundStartTime);
                    startSinePhase(ctx, PHASE1_RATE - NORMAL_RATE,  NORMAL_RATE, NORMAL_TIME + PHASE1_TIME + PHASE2_TIME, roundStartTime);
                    if (!isRunning) {
                        return;
                    }
                }
            }
        }
        public void run(SourceContext<Tuple3<String, Long, Long>> ctx) throws Exception {
            if(withStairFlag){
                generateWithStairCurve(ctx);
            }else{
                generateWithOutStairCurve(ctx);
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
    public static final class ChangingAmplitudeSource implements SourceFunction<Tuple3<String, Long, Long>>, CheckpointedFunction {
        final private boolean withStairFlag;
        final private long WARMP_TIME, WARMP_RATE, NORMAL_TIME, NORMAL_RATE, BIG_PERIOD, MAX_RATE, SMALL_PERIOD, SMALL_RATE, TOTAL_TIME;
        private int count = 0;
        private final int curve_type;
        private volatile boolean isRunning = true;
        private transient ListState<Integer> checkpointedCount;
        private int maxParallelism;
        private FastZipfGenerator fastZipfGenerator;
        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private int nKeys;
        private final Map<Integer, List<String>> keyGroupMapping = new HashMap<>();

        private final Map<Integer, Long> totalOutputNumbers = new HashMap<>();

        public ChangingAmplitudeSource(long WARMP_TIME, long PHASE1_TIME, long PHASE2_TIME, long NORMAL_TIME, long WARMP_RATE, long PHASE1_RATE, long PHASE2_RATE, long NORMAL_RATE, long runtime, String curve_type){
            if(NORMAL_TIME == 0){
                withStairFlag = false;
            }else {
                withStairFlag = true;
            }
            this.WARMP_TIME = WARMP_TIME;
            this.WARMP_RATE = WARMP_RATE;
            this.BIG_PERIOD = PHASE1_TIME;
            this.SMALL_PERIOD = PHASE2_TIME;
            this.NORMAL_TIME = NORMAL_TIME;
            this.MAX_RATE = PHASE1_RATE;
            this.SMALL_RATE = PHASE2_RATE;
            this.NORMAL_RATE = NORMAL_RATE;
            this.TOTAL_TIME = runtime;
            if(curve_type.equals("gradient")) {
                this.curve_type = 0;
            }else if(curve_type.equals("linear")){
                this.curve_type = 1;
            }else if(curve_type.equals("quarter-sine")) {
                this.curve_type = 2;
            }else if(curve_type.equals("sine")){
                this.curve_type = 3;
            }else if(curve_type.equals("mixed")) {
                this.curve_type = 4;
            }else{
                this.curve_type = 0;
            }
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
        void startSteadyPhase(SourceContext<Tuple3<String, Long, Long>> ctx, long rate, long time, long phaseStartTime)throws Exception {
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

        void startSinePhase(SourceContext<Tuple3<String, Long, Long>> ctx, long amplitude, long baseRate, long time, long phaseStartTime) throws Exception {
            long currentTime;
            long elapsedTime;
            long rate;

            while (isRunning && (currentTime = System.currentTimeMillis()) - phaseStartTime < time) {
                long emitStartTime = System.currentTimeMillis();
                elapsedTime = currentTime - phaseStartTime;

                // Calculate the rate using a sine function
                rate = (long) (baseRate + amplitude * Math.sin((2 * Math.PI * elapsedTime) / time));


                for (int i = 0; i < rate / 20; i++) {
                    int selectedKeygroup = fastZipfGenerator.next();
                    List<String> subKeySet = keyGroupMapping.get(selectedKeygroup);
                    totalOutputNumbers.put(selectedKeygroup, totalOutputNumbers.getOrDefault(selectedKeygroup, 0L) + 1);
                    String key = getSubKeySetChar(count, subKeySet);
                    ctx.collect(Tuple3.of(key, System.currentTimeMillis(), (long) count));
                    count++;
                }

                Util.pause(emitStartTime);
            }
        }

        private void generateCurve(SourceContext<Tuple3<String, Long, Long>> ctx) throws Exception {
            long startTime = System.currentTimeMillis();
            System.out.println("Source start at: " + startTime);
            System.out.println("Source warm up...");
            // startSteadyPhase(ctx, NORMAL_RATE, 20 * 1000, startTime);
            startSteadyPhase(ctx, WARMP_RATE, WARMP_TIME, startTime);
            startTime = System.currentTimeMillis();
            long round = 0;
            while (isRunning && System.currentTimeMillis() - startTime < TOTAL_TIME) {
                round++;
                int this_round_curve;
                long passed_time = System.currentTimeMillis() - startTime;
                double ratio;
                if((passed_time / BIG_PERIOD) % 2 == 0){
                    ratio = ((passed_time % BIG_PERIOD) / (double)BIG_PERIOD);
                }else{
                    ratio = 1.0 - ((passed_time % BIG_PERIOD) / (double)BIG_PERIOD);
                }
                long AMPLITUDE = Math.round(ratio * (this.MAX_RATE - this.SMALL_RATE)) + this.SMALL_RATE - NORMAL_RATE;
                if(curve_type != 4){
                    this_round_curve = curve_type;
                }else{
                    this_round_curve = (int)((round - 1) % 4);
                }
                if(this_round_curve == 3){ // Sine
                    long roundStartTime = System.currentTimeMillis();
                    System.out.println("Round " + round + " sine phase start at: " + roundStartTime);
                    startSinePhase(ctx, AMPLITUDE,  NORMAL_RATE, SMALL_PERIOD, roundStartTime);
                    if (!isRunning) {
                        return;
                    }
                }
            }
        }
        public void run(SourceContext<Tuple3<String, Long, Long>> ctx) throws Exception {
            generateCurve(ctx);
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
    public static final class SineWithSpikeSource implements SourceFunction<Tuple3<String, Long, Long>>, CheckpointedFunction {
        final private boolean withStairFlag;
        final private long WARMP_TIME, WARMP_RATE, NORMAL_TIME, NORMAL_RATE, PERIOD, MAX_RATE, SPIKE_LENGTH, SMALL_RATE, TOTAL_TIME;
        private int count = 0;
        private long next_spike_time = 0;
        private final int curve_type;
        private volatile boolean isRunning = true;
        private transient ListState<Integer> checkpointedCount;
        private int maxParallelism;
        private FastZipfGenerator fastZipfGenerator;
        private Random random = new Random(114514);
        private int nKeys;
        private final Map<Integer, List<String>> keyGroupMapping = new HashMap<>();

        private final Map<Integer, Long> totalOutputNumbers = new HashMap<>();

        public SineWithSpikeSource(long WARMP_TIME, long PHASE1_TIME, long PHASE2_TIME, long NORMAL_TIME, long WARMP_RATE, long PHASE1_RATE, long PHASE2_RATE, long NORMAL_RATE, long runtime, String curve_type){
            if(NORMAL_TIME == 0){
                withStairFlag = false;
            }else {
                withStairFlag = true;
            }
            this.WARMP_TIME = WARMP_TIME;
            this.WARMP_RATE = WARMP_RATE;
            this.PERIOD = PHASE1_TIME;
            this.SPIKE_LENGTH = PHASE2_TIME;
            this.NORMAL_TIME = NORMAL_TIME;
            this.MAX_RATE = PHASE1_RATE;
            this.SMALL_RATE = PHASE2_RATE;
            this.NORMAL_RATE = NORMAL_RATE;
            this.TOTAL_TIME = runtime;
            if(curve_type.equals("gradient")) {
                this.curve_type = 0;
            }else if(curve_type.equals("linear")){
                this.curve_type = 1;
            }else if(curve_type.equals("quarter-sine")) {
                this.curve_type = 2;
            }else if(curve_type.equals("sine")){
                this.curve_type = 3;
            }else if(curve_type.equals("mixed")) {
                this.curve_type = 4;
            }else{
                this.curve_type = 0;
            }
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
        void startSteadyPhase(SourceContext<Tuple3<String, Long, Long>> ctx, long rate, long time, long phaseStartTime)throws Exception {
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

        void startSinePhase(SourceContext<Tuple3<String, Long, Long>> ctx, long amplitude, long baseRate, long time, long phaseStartTime) throws Exception {
            long currentTime;
            long elapsedTime;
            long rate;
            long spike_end_time = 0;
            while (isRunning && (currentTime = System.currentTimeMillis()) - phaseStartTime < time) {
                long emitStartTime = System.currentTimeMillis();
                elapsedTime = currentTime - phaseStartTime;
                if(emitStartTime >= next_spike_time && emitStartTime > spike_end_time){
                    rate = (long) this.SMALL_RATE;
                    spike_end_time = emitStartTime + this.SPIKE_LENGTH;
                    next_spike_time = emitStartTime + (random.nextInt(15) + 30) * 1000;
                }else if(emitStartTime <= spike_end_time){
                    rate = (long) this.SMALL_RATE;
                }else {
                    // Calculate the rate using a sine function
                    rate = (long) (baseRate + amplitude * Math.sin((2 * Math.PI * elapsedTime) / time));
                }

                for (int i = 0; i < rate / 20; i++) {
                    int selectedKeygroup = fastZipfGenerator.next();
                    List<String> subKeySet = keyGroupMapping.get(selectedKeygroup);
                    totalOutputNumbers.put(selectedKeygroup, totalOutputNumbers.getOrDefault(selectedKeygroup, 0L) + 1);
                    String key = getSubKeySetChar(count, subKeySet);
                    ctx.collect(Tuple3.of(key, System.currentTimeMillis(), (long) count));
                    count++;
                }

                Util.pause(emitStartTime);
            }
        }

        private void generateCurve(SourceContext<Tuple3<String, Long, Long>> ctx) throws Exception {
            long startTime = System.currentTimeMillis();
            System.out.println("Source start at: " + startTime);
            System.out.println("Source warm up...");
            // startSteadyPhase(ctx, NORMAL_RATE, 20 * 1000, startTime);
            startSteadyPhase(ctx, WARMP_RATE, WARMP_TIME, startTime);
            startTime = System.currentTimeMillis();
            long round = 0;
            next_spike_time = startTime + (random.nextInt(15) + 30) * 1000;
            while (isRunning && System.currentTimeMillis() - startTime < TOTAL_TIME) {
                round++;
                int this_round_curve;
                long AMPLITUDE = this.MAX_RATE - NORMAL_RATE;
                if(curve_type != 4){
                    this_round_curve = curve_type;
                }else{
                    this_round_curve = (int)((round - 1) % 4);
                }
                if(this_round_curve == 3){ // Sine
                    long roundStartTime = System.currentTimeMillis();
                    System.out.println("Round " + round + " sine phase start at: " + roundStartTime);
                    startSinePhase(ctx, AMPLITUDE,  NORMAL_RATE, PERIOD, roundStartTime);
                    if (!isRunning) {
                        return;
                    }
                }
            }
        }
        public void run(SourceContext<Tuple3<String, Long, Long>> ctx) throws Exception {
            generateCurve(ctx);
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
    public static final class ChangingPeriodSource implements SourceFunction<Tuple3<String, Long, Long>>, CheckpointedFunction {
        final private boolean withStairFlag;
        final private long WARMP_TIME, WARMP_RATE, NORMAL_TIME, NORMAL_RATE, BIG_PERIOD, MAX_RATE, SMALL_PERIOD, SMALL_RATE, TOTAL_TIME;
        private int count = 0;
        private final int curve_type;
        private volatile boolean isRunning = true;
        private transient ListState<Integer> checkpointedCount;
        private int maxParallelism;
        private FastZipfGenerator fastZipfGenerator;
        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private int nKeys;
        private final Map<Integer, List<String>> keyGroupMapping = new HashMap<>();

        private final Map<Integer, Long> totalOutputNumbers = new HashMap<>();

        public ChangingPeriodSource(long WARMP_TIME, long PHASE1_TIME, long PHASE2_TIME, long NORMAL_TIME, long WARMP_RATE, long PHASE1_RATE, long PHASE2_RATE, long NORMAL_RATE, long runtime, String curve_type){
            if(NORMAL_TIME == 0){
                withStairFlag = false;
            }else {
                withStairFlag = true;
            }
            this.WARMP_TIME = WARMP_TIME;
            this.WARMP_RATE = WARMP_RATE;
            this.BIG_PERIOD = PHASE1_TIME;
            this.SMALL_PERIOD = PHASE2_TIME;
            this.NORMAL_TIME = NORMAL_TIME;
            this.MAX_RATE = PHASE1_RATE;
            this.SMALL_RATE = PHASE2_RATE;
            this.NORMAL_RATE = NORMAL_RATE;
            this.TOTAL_TIME = runtime;
            if(curve_type.equals("gradient")) {
                this.curve_type = 0;
            }else if(curve_type.equals("linear")){
                this.curve_type = 1;
            }else if(curve_type.equals("quarter-sine")) {
                this.curve_type = 2;
            }else if(curve_type.equals("sine")){
                this.curve_type = 3;
            }else if(curve_type.equals("mixed")) {
                this.curve_type = 4;
            }else{
                this.curve_type = 0;
            }
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
        void startSteadyPhase(SourceContext<Tuple3<String, Long, Long>> ctx, long rate, long time, long phaseStartTime)throws Exception {
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

        void startSinePhase(SourceContext<Tuple3<String, Long, Long>> ctx, long amplitude, long baseRate, long time, long phaseStartTime) throws Exception {
            long currentTime;
            long elapsedTime;
            long rate;

            while (isRunning && (currentTime = System.currentTimeMillis()) - phaseStartTime < time) {
                long emitStartTime = System.currentTimeMillis();
                elapsedTime = currentTime - phaseStartTime;

                // Calculate the rate using a sine function
                rate = (long) (baseRate + amplitude * Math.sin((2 * Math.PI * elapsedTime) / time));


                for (int i = 0; i < rate / 20; i++) {
                    int selectedKeygroup = fastZipfGenerator.next();
                    List<String> subKeySet = keyGroupMapping.get(selectedKeygroup);
                    totalOutputNumbers.put(selectedKeygroup, totalOutputNumbers.getOrDefault(selectedKeygroup, 0L) + 1);
                    String key = getSubKeySetChar(count, subKeySet);
                    ctx.collect(Tuple3.of(key, System.currentTimeMillis(), (long) count));
                    count++;
                }

                Util.pause(emitStartTime);
            }
        }

        private void generateCurve(SourceContext<Tuple3<String, Long, Long>> ctx) throws Exception {
            long startTime = System.currentTimeMillis();
            System.out.println("Source start at: " + startTime);
            System.out.println("Source warm up...");
            // startSteadyPhase(ctx, NORMAL_RATE, 20 * 1000, startTime);
            startSteadyPhase(ctx, WARMP_RATE, WARMP_TIME, startTime);
            startTime = System.currentTimeMillis();
            long round = 0;
            while (isRunning && System.currentTimeMillis() - startTime < TOTAL_TIME) {
                round++;
                int this_round_curve;
                long passed_time = System.currentTimeMillis() - startTime;
                double ratio;
                ratio = (passed_time / (double)TOTAL_TIME);
                long AMPLITUDE = this.MAX_RATE - this.NORMAL_RATE;
                long PERIOD = this.BIG_PERIOD - Math.round(ratio * (this.BIG_PERIOD - this.SMALL_PERIOD));
                if(curve_type != 4){
                    this_round_curve = curve_type;
                }else{
                    this_round_curve = (int)((round - 1) % 4);
                }
                if(this_round_curve == 3){ // Sine
                    long roundStartTime = System.currentTimeMillis();
                    System.out.println("Round " + round + " sine phase start at: " + roundStartTime);
                    startSinePhase(ctx, AMPLITUDE,  NORMAL_RATE, PERIOD, roundStartTime);
                    if (!isRunning) {
                        return;
                    }
                }
            }
        }
        public void run(SourceContext<Tuple3<String, Long, Long>> ctx) throws Exception {
            generateCurve(ctx);
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
    public static final class SinePhaseShift implements SourceFunction<Tuple3<String, Long, Long>>, CheckpointedFunction {
        final private boolean withStairFlag;
        final private long WARMP_TIME, WARMP_RATE, NORMAL_TIME, NORMAL_RATE, PHASE1_PERIOD, INITIAL_MAX_RATE, PHASE2_PERIOD, PHASE2_RATE, TOTAL_TIME;
        final private double amplitude_ratio;
        private int count = 0;
        private final int curve_type;
        private volatile boolean isRunning = true;
        private transient ListState<Integer> checkpointedCount;
        private int maxParallelism;
        private FastZipfGenerator fastZipfGenerator;
        private Random random = new Random(114514);
        private int nKeys;
        private final Map<Integer, List<String>> keyGroupMapping = new HashMap<>();

        private final Map<Integer, Long> totalOutputNumbers = new HashMap<>();

        public SinePhaseShift(long WARMP_TIME, long PHASE1_TIME, long PHASE2_TIME, long NORMAL_TIME, long WARMP_RATE, long PHASE1_RATE, long PHASE2_RATE, long NORMAL_RATE, long runtime, String curve_type){
            if(NORMAL_TIME == 0){
                withStairFlag = false;
            }else {
                withStairFlag = true;
            }
            this.WARMP_TIME = WARMP_TIME;
            this.WARMP_RATE = WARMP_RATE;
            this.PHASE1_PERIOD = PHASE1_TIME;
            this.PHASE2_PERIOD = PHASE2_TIME;
            this.NORMAL_TIME = NORMAL_TIME;
            this.INITIAL_MAX_RATE = PHASE1_RATE;
            this.amplitude_ratio = (INITIAL_MAX_RATE - NORMAL_RATE)/(double) NORMAL_RATE;
            this.PHASE2_RATE = PHASE2_RATE;
            this.NORMAL_RATE = NORMAL_RATE;
            this.TOTAL_TIME = runtime;
            if(curve_type.equals("gradient")) {
                this.curve_type = 0;
            }else if(curve_type.equals("linear")){
                this.curve_type = 1;
            }else if(curve_type.equals("quarter-sine")) {
                this.curve_type = 2;
            }else if(curve_type.equals("sine")){
                this.curve_type = 3;
            }else if(curve_type.equals("mixed")) {
                this.curve_type = 4;
            }else{
                this.curve_type = 0;
            }
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
        void startSteadyPhase(SourceContext<Tuple3<String, Long, Long>> ctx, long rate, long time, long phaseStartTime)throws Exception {
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

        void startSinePhase(SourceContext<Tuple3<String, Long, Long>> ctx, long amplitude, long baseRate, long time, long phaseStartTime) throws Exception {
            long currentTime;
            long elapsedTime;
            long rate;
            while (isRunning && (currentTime = System.currentTimeMillis()) - phaseStartTime < time) {
                long emitStartTime = System.currentTimeMillis();
                elapsedTime = currentTime - phaseStartTime;
                rate = (long) (baseRate + amplitude * Math.sin((2 * Math.PI * elapsedTime) / time));
                for (int i = 0; i < rate / 20; i++) {
                    int selectedKeygroup = fastZipfGenerator.next();
                    List<String> subKeySet = keyGroupMapping.get(selectedKeygroup);
                    totalOutputNumbers.put(selectedKeygroup, totalOutputNumbers.getOrDefault(selectedKeygroup, 0L) + 1);
                    String key = getSubKeySetChar(count, subKeySet);
                    ctx.collect(Tuple3.of(key, System.currentTimeMillis(), (long) count));
                    count++;
                }

                Util.pause(emitStartTime);
            }
        }

        private void generateCurve(SourceContext<Tuple3<String, Long, Long>> ctx) throws Exception {
            long startTime = System.currentTimeMillis();
            System.out.println("Source start at: " + startTime);
            System.out.println("Source warm up...");
            // startSteadyPhase(ctx, NORMAL_RATE, 20 * 1000, startTime);
            startSteadyPhase(ctx, WARMP_RATE, WARMP_TIME, startTime);
            startTime = System.currentTimeMillis();
            long round = 0;
            while (isRunning && System.currentTimeMillis() - startTime < TOTAL_TIME) {
                round++;
                int this_round_curve;
                if(curve_type != 4){
                    this_round_curve = curve_type;
                }else{
                    this_round_curve = (int)((round - 1) % 4);
                }
                if(this_round_curve == 3){ // Sine
                    long roundStartTime = System.currentTimeMillis();
                    System.out.println("Round " + round + " sine phase start at: " + roundStartTime);
                    if(System.currentTimeMillis() - startTime <= TOTAL_TIME / 2) {
                        long AMPLITUDE = Math.round(NORMAL_RATE * amplitude_ratio);
                        startSinePhase(ctx, AMPLITUDE, NORMAL_RATE, PHASE1_PERIOD, roundStartTime);
                    }else{
                        long AMPLITUDE = Math.round(PHASE2_RATE * amplitude_ratio);
                        startSinePhase(ctx, AMPLITUDE, PHASE2_RATE, PHASE2_PERIOD, roundStartTime);
                    }
                    if (!isRunning) {
                        return;
                    }
                }
            }
        }
        public void run(SourceContext<Tuple3<String, Long, Long>> ctx) throws Exception {
            generateCurve(ctx);
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
    public static final class SineTwoPhase implements SourceFunction<Tuple3<String, Long, Long>>, CheckpointedFunction {
        final private boolean withStairFlag;
        final private long WARMP_TIME, WARMP_RATE, NORMAL_TIME, NORMAL_RATE, PHASE1_PERIOD, PHASE1_RATE, PHASE2_PERIOD, PHASE2_RATE, TOTAL_TIME;
        private int count = 0;
        private final int curve_type;
        private volatile boolean isRunning = true;
        private transient ListState<Integer> checkpointedCount;
        private int maxParallelism;
        private FastZipfGenerator fastZipfGenerator;
        private Random random = new Random(114514);
        private int nKeys;
        private final Map<Integer, List<String>> keyGroupMapping = new HashMap<>();

        private final Map<Integer, Long> totalOutputNumbers = new HashMap<>();

        public SineTwoPhase(long WARMP_TIME, long PHASE1_TIME, long PHASE2_TIME, long NORMAL_TIME, long WARMP_RATE, long PHASE1_RATE, long PHASE2_RATE, long NORMAL_RATE, long runtime, String curve_type){
            if(NORMAL_TIME == 0){
                withStairFlag = false;
            }else {
                withStairFlag = true;
            }
            this.WARMP_TIME = WARMP_TIME;
            this.WARMP_RATE = WARMP_RATE;
            this.PHASE1_PERIOD = PHASE1_TIME;
            this.PHASE2_PERIOD = PHASE2_TIME;
            this.NORMAL_TIME = NORMAL_TIME;
            this.PHASE1_RATE = PHASE1_RATE;
            this.PHASE2_RATE = PHASE2_RATE;
            this.NORMAL_RATE = NORMAL_RATE;
            this.TOTAL_TIME = runtime;
            if(curve_type.equals("gradient")) {
                this.curve_type = 0;
            }else if(curve_type.equals("linear")){
                this.curve_type = 1;
            }else if(curve_type.equals("quarter-sine")) {
                this.curve_type = 2;
            }else if(curve_type.equals("sine")){
                this.curve_type = 3;
            }else if(curve_type.equals("mixed")) {
                this.curve_type = 4;
            }else{
                this.curve_type = 0;
            }
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
        void startSteadyPhase(SourceContext<Tuple3<String, Long, Long>> ctx, long rate, long time, long phaseStartTime)throws Exception {
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

        void startSinePhase(SourceContext<Tuple3<String, Long, Long>> ctx, long amplitude, long baseRate, long time, long phaseStartTime) throws Exception {
            long currentTime;
            long elapsedTime;
            long rate;
            while (isRunning && (currentTime = System.currentTimeMillis()) - phaseStartTime < time) {
                long emitStartTime = System.currentTimeMillis();
                elapsedTime = currentTime - phaseStartTime;
                rate = (long) (baseRate + amplitude * Math.sin((2 * Math.PI * elapsedTime) / time));
                for (int i = 0; i < rate / 20; i++) {
                    int selectedKeygroup = fastZipfGenerator.next();
                    List<String> subKeySet = keyGroupMapping.get(selectedKeygroup);
                    totalOutputNumbers.put(selectedKeygroup, totalOutputNumbers.getOrDefault(selectedKeygroup, 0L) + 1);
                    String key = getSubKeySetChar(count, subKeySet);
                    ctx.collect(Tuple3.of(key, System.currentTimeMillis(), (long) count));
                    count++;
                }

                Util.pause(emitStartTime);
            }
        }

        private void generateCurve(SourceContext<Tuple3<String, Long, Long>> ctx) throws Exception {
            long startTime = System.currentTimeMillis();
            System.out.println("Source start at: " + startTime);
            System.out.println("Source warm up...");
            // startSteadyPhase(ctx, NORMAL_RATE, 20 * 1000, startTime);
            startSteadyPhase(ctx, WARMP_RATE, WARMP_TIME, startTime);
            startTime = System.currentTimeMillis();
            long round = 0;
            while (isRunning && System.currentTimeMillis() - startTime < TOTAL_TIME) {
                round++;
                int this_round_curve;
                if(curve_type != 4){
                    this_round_curve = curve_type;
                }else{
                    this_round_curve = (int)((round - 1) % 4);
                }
                if(this_round_curve == 3){ // Sine
                    long roundStartTime = System.currentTimeMillis();
                    System.out.println("Round " + round + " sine phase start at: " + roundStartTime);
                    if(System.currentTimeMillis() - startTime <= TOTAL_TIME / 2) {
                        long AMPLITUDE = PHASE1_RATE - NORMAL_RATE;
                        startSinePhase(ctx, AMPLITUDE, NORMAL_RATE, PHASE1_PERIOD, roundStartTime);
                    }else{
                        long AMPLITUDE = PHASE2_RATE - NORMAL_RATE;
                        startSinePhase(ctx, AMPLITUDE, PHASE2_RATE, PHASE2_PERIOD, roundStartTime);
                    }
                    if (!isRunning) {
                        return;
                    }
                }
            }
        }
        public void run(SourceContext<Tuple3<String, Long, Long>> ctx) throws Exception {
            generateCurve(ctx);
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
    public static final class LinearChangingAveragePhase implements SourceFunction<Tuple3<String, Long, Long>>, CheckpointedFunction {
        final private boolean withStairFlag;
        final private long WARMP_TIME, WARMP_RATE, NORMAL_TIME, NORMAL_RATE, BIG_PERIOD, MAX_RATE, PHASE2_PERIOD, PHASE2_RATE, TOTAL_TIME;
        final private double amplitude_ratio;
        private int count = 0;
        private final int curve_type;
        private volatile boolean isRunning = true;
        private transient ListState<Integer> checkpointedCount;
        private int maxParallelism;
        private FastZipfGenerator fastZipfGenerator;
        private Random random = new Random(114514);
        private int nKeys;
        private final Map<Integer, List<String>> keyGroupMapping = new HashMap<>();

        private final Map<Integer, Long> totalOutputNumbers = new HashMap<>();

        public LinearChangingAveragePhase(long WARMP_TIME, long PHASE1_TIME, long PHASE2_TIME, long NORMAL_TIME, long WARMP_RATE, long PHASE1_RATE, long PHASE2_RATE, long NORMAL_RATE, long runtime, String curve_type){
            if(NORMAL_TIME == 0){
                withStairFlag = false;
            }else {
                withStairFlag = true;
            }
            this.WARMP_TIME = WARMP_TIME;
            this.WARMP_RATE = WARMP_RATE;
            this.BIG_PERIOD = PHASE1_TIME;
            this.PHASE2_PERIOD = PHASE2_TIME;
            this.NORMAL_TIME = NORMAL_TIME;
            this.MAX_RATE = PHASE1_RATE;
            this.amplitude_ratio = (MAX_RATE - NORMAL_RATE)/(double) NORMAL_RATE;
            this.PHASE2_RATE = PHASE2_RATE;
            this.NORMAL_RATE = NORMAL_RATE;
            this.TOTAL_TIME = runtime;
            if(curve_type.equals("gradient")) {
                this.curve_type = 0;
            }else if(curve_type.equals("linear")){
                this.curve_type = 1;
            }else if(curve_type.equals("quarter-sine")) {
                this.curve_type = 2;
            }else if(curve_type.equals("sine")){
                this.curve_type = 3;
            }else if(curve_type.equals("mixed")) {
                this.curve_type = 4;
            }else{
                this.curve_type = 0;
            }
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
        void startSteadyPhase(SourceContext<Tuple3<String, Long, Long>> ctx, long rate, long time, long phaseStartTime)throws Exception {
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

        void startLinearPhase(SourceContext<Tuple3<String, Long, Long>> ctx, long rate1, long rate2, long time, long phaseStartTime)throws Exception {
            long currentTime;
            long elapsedTime;
            long rate;
            double noiseLevel = 0.05;  // 10% noise level

            while (isRunning && (currentTime = System.currentTimeMillis()) - phaseStartTime < time) {
                long emitStartTime = System.currentTimeMillis();
                elapsedTime = currentTime - phaseStartTime;

                // Calculate the current rate using linear interpolation between rate1 and rate2
                rate = rate1 + ((rate2 - rate1) * elapsedTime) / time;

                // Add Gaussian noise to the rate, scaled by noise level
                double noise = random.nextGaussian() * noiseLevel;
                rate = (long) (rate * (1 + noise));  // Apply the noise

                // Ensure that the rate is not negative after applying noise
                if (rate < 0) {
                    rate = 0;
                }

                for (int i = 0; i < rate / 20; i++) {
                    int selectedKeygroup = fastZipfGenerator.next();
                    List<String> subKeySet = keyGroupMapping.get(selectedKeygroup);
                    totalOutputNumbers.put(selectedKeygroup, totalOutputNumbers.getOrDefault(selectedKeygroup, 0L) + 1);
                    String key = getSubKeySetChar(count, subKeySet);
                    ctx.collect(Tuple3.of(key, System.currentTimeMillis(), (long) count));
                    count++;
                }

                Util.pause(emitStartTime);
            }
        }

        void startSinePhase(SourceContext<Tuple3<String, Long, Long>> ctx, long amplitude, long baseRate, long time, long phaseStartTime) throws Exception {
            long currentTime;
            long elapsedTime;
            long rate;
            double noiseLevel = 0.05;  // 10% noise level

            while (isRunning && (currentTime = System.currentTimeMillis()) - phaseStartTime < time) {
                long emitStartTime = System.currentTimeMillis();
                elapsedTime = currentTime - phaseStartTime;

                // Calculate the sine wave rate
                rate = (long) (baseRate + amplitude * Math.sin((2 * Math.PI * elapsedTime) / time));

                // Add Gaussian noise to the rate, scaled by noise level
                double noise = random.nextGaussian() * noiseLevel;
                rate = (long) (rate * (1 + noise));  // Apply the noise

                // Ensure that the rate is not negative after applying noise
                if (rate < 0) {
                    rate = 0;
                }

                for (int i = 0; i < rate / 20; i++) {
                    int selectedKeygroup = fastZipfGenerator.next();
                    List<String> subKeySet = keyGroupMapping.get(selectedKeygroup);
                    totalOutputNumbers.put(selectedKeygroup, totalOutputNumbers.getOrDefault(selectedKeygroup, 0L) + 1);
                    String key = getSubKeySetChar(count, subKeySet);
                    ctx.collect(Tuple3.of(key, System.currentTimeMillis(), (long) count));
                    count++;
                }

                Util.pause(emitStartTime);
            }
        }

        private void generateCurve(SourceContext<Tuple3<String, Long, Long>> ctx) throws Exception {
            long startTime = System.currentTimeMillis();
            long [] PERIOD_CHOICE = {30, 45, 60, 90};
            double [] AMPLITUDE_CHOICE = {0.2, 0.25, 0.3, 0.35, 0.4};
            System.out.println("Source start at: " + startTime);
            System.out.println("Source warm up...");
            // startSteadyPhase(ctx, NORMAL_RATE, 20 * 1000, startTime);
            startSteadyPhase(ctx, WARMP_RATE, WARMP_TIME, startTime);
            startTime = System.currentTimeMillis();
            long round = 0;
            while (isRunning && System.currentTimeMillis() - startTime < TOTAL_TIME) {
                round++;
                long roundStartTime = System.currentTimeMillis();
                double ratio;
                if (((roundStartTime - startTime) / (BIG_PERIOD/2)) % 2 == 0){
                    ratio = ((roundStartTime - startTime) % (BIG_PERIOD / 2)) / (double)(BIG_PERIOD/2);
                }else{
                    ratio = 1.0 - ((roundStartTime - startTime) % (BIG_PERIOD / 2)) / (double)(BIG_PERIOD/2);
                }
                long avg_rate = Math.round(ratio * (MAX_RATE - NORMAL_RATE) + NORMAL_RATE);
                int curve_type = random.nextInt(2), period_option = random.nextInt(4), amplitude_option = random.nextInt(5);
                long period = PERIOD_CHOICE[period_option] * 1000;
                long AMPLITUDE = Math.round(avg_rate * AMPLITUDE_CHOICE[amplitude_option]);
                if(curve_type == 0){
                    System.out.println("Round " + round + " sine phase start at: " + roundStartTime);
                    startSinePhase(ctx, AMPLITUDE, avg_rate, period, roundStartTime);
                }else{
                    System.out.println("Round " + round + " linear phase 1 start at: " + roundStartTime);
                    startLinearPhase(ctx, avg_rate, avg_rate + AMPLITUDE, period / 2, roundStartTime);
                    if (!isRunning) {
                        return;
                    }
                    roundStartTime = System.currentTimeMillis();
                    System.out.println("Round " + round + " linear phase 2 start at: " + roundStartTime);
                    startLinearPhase(ctx, avg_rate + AMPLITUDE, avg_rate, period / 2, roundStartTime);
                }

                if (!isRunning) {
                    return;
                }
            }
        }
        public void run(SourceContext<Tuple3<String, Long, Long>> ctx) throws Exception {
            generateCurve(ctx);
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
    public static final class ChangingAmplitudeAndPeriodSource implements SourceFunction<Tuple3<String, Long, Long>>, CheckpointedFunction {
        final private boolean withStairFlag;
        final private long WARMP_TIME, WARMP_RATE, NORMAL_TIME, NORMAL_RATE, INITIAL_PERIOD, INITIAL_RATE, END_PERIOD, END_RATE, TOTAL_TIME;
        private int count = 0;
        private final int curve_type;
        private volatile boolean isRunning = true;
        private transient ListState<Integer> checkpointedCount;
        private int maxParallelism;
        private FastZipfGenerator fastZipfGenerator;
        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private int nKeys;
        private final Map<Integer, List<String>> keyGroupMapping = new HashMap<>();

        private final Map<Integer, Long> totalOutputNumbers = new HashMap<>();

        public ChangingAmplitudeAndPeriodSource(long WARMP_TIME, long PHASE1_TIME, long PHASE2_TIME, long NORMAL_TIME, long WARMP_RATE, long PHASE1_RATE, long PHASE2_RATE, long NORMAL_RATE, long runtime, String curve_type){
            if(NORMAL_TIME == 0){
                withStairFlag = false;
            }else {
                withStairFlag = true;
            }
            this.WARMP_TIME = WARMP_TIME;
            this.WARMP_RATE = WARMP_RATE;
            this.INITIAL_PERIOD = PHASE1_TIME;
            this.END_PERIOD = PHASE2_TIME;
            this.NORMAL_TIME = NORMAL_TIME;
            this.INITIAL_RATE = PHASE1_RATE;
            this.END_RATE = PHASE2_RATE;
            this.NORMAL_RATE = NORMAL_RATE;
            this.TOTAL_TIME = runtime;
            if(curve_type.equals("gradient")) {
                this.curve_type = 0;
            }else if(curve_type.equals("linear")){
                this.curve_type = 1;
            }else if(curve_type.equals("quarter-sine")) {
                this.curve_type = 2;
            }else if(curve_type.equals("sine")){
                this.curve_type = 3;
            }else if(curve_type.equals("mixed")) {
                this.curve_type = 4;
            }else{
                this.curve_type = 0;
            }
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
        void startSteadyPhase(SourceContext<Tuple3<String, Long, Long>> ctx, long rate, long time, long phaseStartTime)throws Exception {
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

        void startSinePhase(SourceContext<Tuple3<String, Long, Long>> ctx, long amplitude, long baseRate, long time, long phaseStartTime) throws Exception {
            long currentTime;
            long elapsedTime;
            long rate;

            while (isRunning && (currentTime = System.currentTimeMillis()) - phaseStartTime < time) {
                long emitStartTime = System.currentTimeMillis();
                elapsedTime = currentTime - phaseStartTime;

                // Calculate the rate using a sine function
                rate = (long) (baseRate + amplitude * Math.sin((2 * Math.PI * elapsedTime) / time));


                for (int i = 0; i < rate / 20; i++) {
                    int selectedKeygroup = fastZipfGenerator.next();
                    List<String> subKeySet = keyGroupMapping.get(selectedKeygroup);
                    totalOutputNumbers.put(selectedKeygroup, totalOutputNumbers.getOrDefault(selectedKeygroup, 0L) + 1);
                    String key = getSubKeySetChar(count, subKeySet);
                    ctx.collect(Tuple3.of(key, System.currentTimeMillis(), (long) count));
                    count++;
                }

                Util.pause(emitStartTime);
            }
        }

        private void generateCurve(SourceContext<Tuple3<String, Long, Long>> ctx) throws Exception {
            long startTime = System.currentTimeMillis();
            System.out.println("Source start at: " + startTime);
            System.out.println("Source warm up...");
            // startSteadyPhase(ctx, NORMAL_RATE, 20 * 1000, startTime);
            startSteadyPhase(ctx, WARMP_RATE, WARMP_TIME, startTime);
            startTime = System.currentTimeMillis();
            long round = 0;
            while (isRunning && System.currentTimeMillis() - startTime < TOTAL_TIME) {
                round++;
                int this_round_curve;
                long passed_time = System.currentTimeMillis() - startTime;
                double ratio = (passed_time / (double)TOTAL_TIME);
                long AMPLITUDE = (this.INITIAL_RATE - Math.round(ratio * (this.INITIAL_RATE - this.END_RATE))) - NORMAL_RATE;
                long PERIOD = this.INITIAL_PERIOD - Math.round(ratio * (this.INITIAL_PERIOD - this.END_PERIOD));
                if(curve_type != 4){
                    this_round_curve = curve_type;
                }else{
                    this_round_curve = (int)((round - 1) % 4);
                }
                if(this_round_curve == 3){ // Sine
                    long roundStartTime = System.currentTimeMillis();
                    System.out.println("Round " + round + " sine phase start at: " + roundStartTime);
                    startSinePhase(ctx, AMPLITUDE,  NORMAL_RATE, PERIOD, roundStartTime);
                    if (!isRunning) {
                        return;
                    }
                }
            }
        }
        public void run(SourceContext<Tuple3<String, Long, Long>> ctx) throws Exception {
            generateCurve(ctx);
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
