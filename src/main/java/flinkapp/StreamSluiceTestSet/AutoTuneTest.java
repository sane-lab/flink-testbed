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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MathUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AutoTuneTest {
    public static void main(String[] args) throws Exception {
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        env.setStateBackend(new MemoryStateBackend(1073741824));
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        List<Long> RATES = new ArrayList<>(), TIMES = new ArrayList<>(), PERIODS = new ArrayList<>();
        int phaseIndex = 0;
        while(true){
            phaseIndex++;
            String ratePara = "phase" + phaseIndex + "Rate";
            String timePara = "phase" + phaseIndex + "Time";
            String periodPara = "phase" + phaseIndex + "Period";
            if(params.has(ratePara)){
                long rate = params.getLong(ratePara, 400), time = params.getLong(timePara, 30) * 1000, period = params.getLong(periodPara, 30) * 1000;
                RATES.add(rate);
                TIMES.add(time);
                PERIODS.add(period);
            }else{
                break;
            }
        }

        final double zipf_skew = params.getDouble("zipf_skew", 0);
        final int nKeys = params.getInt("nkeys", 1000);
        env.addSource(new MultiPhasePeriodicalSource(params.getLong("warmupTime", 20) * 1000, params.getLong("warmupRate", 400), TIMES, RATES, PERIODS , params.getInt("mp2", 8), zipf_skew, nKeys, params.get("curve_type", "sine")))
                .setParallelism(params.getInt("p1", 1))
                .keyBy(0)
                .flatMap(new DumbStatefulMap(params.getLong("op2Delay", 100), params.getInt("op2IoRate", 1), params.getInt("op2KeyStateSize", 1)))
                .disableChaining()
                .name("Splitter")
                .uid("op2")
                .setParallelism(params.getInt("p2", 1))
                .setMaxParallelism(params.getInt("mp2", 8))
                .slotSharingGroup("g2").keyBy(0)
                .flatMap(new DumbStatefulMap(params.getLong("op3Delay", 100), params.getInt("op3IoRate", 1), params.getInt("op3KeyStateSize", 1)))
                .disableChaining()
                .name("FlatMap 3")
                .uid("op3")
                .setParallelism(params.getInt("p3", 1))
                .setMaxParallelism(params.getInt("mp3", 8))
                .slotSharingGroup("g3")
                .keyBy(0)
                .flatMap(new DumbStatefulMap(params.getLong("op4Delay", 100), params.getInt("op4IoRate", 1), params.getInt("op4KeyStateSize", 1)))
                .disableChaining()
                .name("FlatMap 4")
                .uid("op4")
                .setParallelism(params.getInt("p4", 1))
                .setMaxParallelism(params.getInt("mp4", 8))
                .slotSharingGroup("g4")
                .keyBy(0)
                .map(new DumbSink(params.getLong("op5Delay", 100), params.getInt("op5KeyStateSize", 1), params.getBoolean("outputGroundTruth", true)))
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


    public static final class MultiPhasePeriodicalSource implements SourceFunction<Tuple3<String, Long, Long>>, CheckpointedFunction {
        private long WARMUP_TIME, WARMUP_RATE;
        private List<Long> TIMES, RATES, PERIODS;
        private long INTERVAL;
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

        public MultiPhasePeriodicalSource(long WARMUP_TIME, long WARMUP_RATE, List<Long> TIMES, List<Long> RATES, List<Long> PERIODS, int maxParallelism, double zipfSkew, int nkeys, String curve_type){
            this.WARMUP_TIME = WARMUP_TIME;
            this.WARMUP_RATE = WARMUP_RATE;
            this.TIMES = TIMES;
            this.RATES = RATES;
            this.PERIODS = PERIODS;
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

        public void startInterPhase(long startTime, SourceContext<Tuple3<String, Long, Long>> ctx, String curve_type, long PHASE1_RATE, long INTERMEDIATE_RATE, long INTERMEDIATE_PERIOD, long INTERMEDIATE_TIME, long macroAmplitude, long macroPeriod) throws Exception {
            long remainedNumber = (long) Math.floor(PHASE1_RATE * INTERVAL / 1000.0);
            long AMPLITUDE;
            while (isRunning && System.currentTimeMillis() - startTime < INTERMEDIATE_TIME) {
                if (remainedNumber <= 0) {
                    long index = (System.currentTimeMillis() - startTime) / INTERVAL;
                    long ntime = (index + 1) * INTERVAL + startTime;
                    double macroTheta = Math.sin(Math.toRadians(index * INTERVAL * 360 / ((double) macroPeriod)));
                    AMPLITUDE = (INTERMEDIATE_RATE - PHASE1_RATE) + (long) Math.floor(macroTheta * macroAmplitude);
                    double theta = 0.0;
                    if (curve_type.equals("sine")) {
                        theta = Math.sin(Math.toRadians(index * INTERVAL * 360 / ((double) INTERMEDIATE_PERIOD) - 90));
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

        public void startStairs(long startTime, SourceContext<Tuple3<String, Long, Long>> ctx, String curve_type, long low_RATE, long high_RATE, long stair_period, long stair_time) throws Exception {
            int stair_num = Integer.parseInt(curve_type.split("_")[2]);
            long stair_gap = Long.parseLong(curve_type.split("_")[1]);
            long [] ratePerStair = new long[stair_num * 2];
            for(int i = 0; i < stair_num; i++){
                ratePerStair[i] = low_RATE + i * stair_gap;
                ratePerStair[stair_num * 2 - i - 1] = low_RATE + i * stair_gap;
            }
            long remainedNumber = ratePerStair[0] * INTERVAL / 1000;
            while (isRunning && System.currentTimeMillis() - startTime < stair_time) {
                if (remainedNumber <= 0) {
                    long index = (System.currentTimeMillis() - startTime) / INTERVAL;
                    long ntime = (index + 1) * INTERVAL + startTime;
                    int stairIndex = (int) ((index * INTERVAL % stair_period) / (stair_period / (stair_num * 2)));
                    remainedNumber = ratePerStair[stairIndex] * INTERVAL / 1000;
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
            // Phase warmup
            long startTime = System.currentTimeMillis();
            System.out.println("Warmup start at: " + startTime);
            while (isRunning && System.currentTimeMillis() - startTime < WARMUP_TIME) {
                long emitStartTime = System.currentTimeMillis();
                for (int i = 0; i < WARMUP_RATE / 20; i++) {
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

            for(int phaseIndex = 0; phaseIndex < RATES.size(); phaseIndex ++) {
                // Phase
                startTime = System.currentTimeMillis();
                System.out.println("Phase " + phaseIndex + " start at: " + startTime);
                if(curve_type.startsWith("stairs_")) {
                    startStairs(startTime, ctx, curve_type, WARMUP_RATE, RATES.get(phaseIndex), PERIODS.get(phaseIndex), TIMES.get(phaseIndex));
                }else{
                    startInterPhase(startTime, ctx, curve_type, WARMUP_RATE, RATES.get(phaseIndex), PERIODS.get(phaseIndex), TIMES.get(phaseIndex), 0, 1);
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
