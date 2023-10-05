package flinkapp.StreamSluiceTestSet;

import Nexmark.sources.Util;
import common.FastZipfGenerator;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.MathUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OneOperatorTest {
    public static void main(String[] args) throws Exception {
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        env.setStateBackend(new MemoryStateBackend(100000000));
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.enableCheckpointing(1000);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        FlinkKafkaProducer011<String> kafkaProducer = new FlinkKafkaProducer011<String>(
//                "localhost:9092", "my-flink-demo-topic0", new SimpleStringSchema());
//        kafkaProducer.setWriteTimestampToKafka(true);

        final long PHASE1_RATE = params.getLong("phase1Rate", 400);
        final long PHASE2_RATE = params.getLong("phase2Rate", 600);
        final long INTERMEDIATE_RATE = params.getLong("interRate", 500);
        final long PHASE1_TIME = params.getLong("phase1Time", 60) * 1000;
        final long PHASE2_TIME = params.getLong("phase2Time", 60) * 1000;
        final long INTERMEDIATE_TIME = params.getLong("interTime", 120) * 1000;
        final long INTERMEDIATE_PERIOD = params.getLong("interPeriod", 240) * 1000;
        final int p1 = params.getInt("p1", 1);
        final int mp1 = params.getInt("mp1", 64);
        final double zipf_skew = params.getDouble("zipf_skew", 0);
        final int stateSize = params.getInt("state_size", 10);
        env.addSource(new TwoPhaseSineSource(PHASE1_TIME, PHASE2_TIME, INTERMEDIATE_TIME, PHASE1_RATE, PHASE2_RATE, INTERMEDIATE_RATE, INTERMEDIATE_PERIOD, mp1, zipf_skew))
                .keyBy(0)
                .map(new DumbStatefulMap(stateSize))
                .disableChaining()
                .name("Time counter")
                .setMaxParallelism(mp1)
                .setParallelism(p1)
                .slotSharingGroup("a");

        env.execute();
    }

    public static final class DumbStatefulMap extends RichMapFunction<Tuple2<String, Long>, Tuple3<String, Long, Long>> {

        private final int stateSize;
        private transient MapState<Integer, Long> countMap;
        private RandomDataGenerator randomGen = new RandomDataGenerator();
        DumbStatefulMap(int _stateSize){
            stateSize = _stateSize;
        }

        @Override
        public Tuple3<String, Long, Long> map(Tuple2<String, Long> input) throws Exception {
            int hashValue = (input.f0.hashCode() % stateSize + stateSize) % stateSize;
            Long cur = countMap.get(hashValue);
            cur = (cur == null) ? 1 : cur + 1;
            countMap.put(hashValue, cur);
            delay(10);
            long currentTime = System.currentTimeMillis();
            System.out.println("GT: " + input.f0 + ", " + currentTime + ", " + (currentTime - input.f1));
            return new Tuple3<String, Long, Long>(input.f0, currentTime, currentTime - input.f1);
        }
        private void delay(int interval) {
            Double ranN = randomGen.nextGaussian(interval, 1);
            ranN = ranN*1000000;
            long delay = ranN.intValue();
            if (delay < 0) delay = interval * 1000000;
            Long start = System.nanoTime();
            while (System.nanoTime() - start < delay) {}
        }

        @Override
        public void open(Configuration config) {
            MapStateDescriptor<Integer, Long> descriptor =
                    new MapStateDescriptor<>("word-count", Integer.class, Long.class);
            countMap = getRuntimeContext().getMapState(descriptor);
        }
    }


    public static final class TwoPhaseSineSource implements SourceFunction<Tuple2<String, Long>>, CheckpointedFunction {
        private long PHASE1_TIME, PHASE2_TIME, INTERMEDIATE_TIME, PHASE1_RATE, PHASE2_RATE, INTERMEDIATE_RATE, INTERMEDIATE_PERIOD, INTERVAL;
        private int count = 0;
        private volatile boolean isRunning = true;

        private transient ListState<Integer> checkpointedCount;

        private int maxParallelism;
        private FastZipfGenerator fastZipfGenerator;
        private final Map<Integer, List<String>> keyGroupMapping = new HashMap<>();

        public TwoPhaseSineSource(long PHASE1_TIME, long PHASE2_TIME, long INTERMEDIATE_TIME, long PHASE1_RATE, long PHASE2_RATE, long INTERMEDIATE_RATE, long INTERMEDIATE_PERIOD, int maxParallelism, double zipfSkew){
            this.PHASE1_TIME = PHASE1_TIME;
            this.PHASE2_TIME = PHASE2_TIME;
            this.INTERMEDIATE_TIME = INTERMEDIATE_TIME;
            this.PHASE1_RATE = PHASE1_RATE;
            this.PHASE2_RATE = PHASE2_RATE;
            this.INTERMEDIATE_RATE = INTERMEDIATE_RATE;
            this.INTERMEDIATE_PERIOD = INTERMEDIATE_PERIOD;
            this.INTERVAL = 50;
            this.maxParallelism = maxParallelism;
            this.fastZipfGenerator = new FastZipfGenerator(maxParallelism, zipfSkew, 0, 114514);
            for (int i = 0; i < 1000; i++) {
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

        public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
            // Phase 1
            List<String> subKeySet;
            long startTime = System.currentTimeMillis();
            System.out.println("Phase 1 start at: " + startTime);
            while (isRunning && System.currentTimeMillis() - startTime < PHASE1_TIME) {
                long emitStartTime = System.currentTimeMillis();
                for (int i = 0; i < PHASE1_RATE / 20; i++) {
                    int selectedKeygroup = fastZipfGenerator.next();
                    subKeySet = keyGroupMapping.get(selectedKeygroup);

                    String key = getSubKeySetChar(count, subKeySet);

                    ctx.collect(Tuple2.of(key, System.currentTimeMillis()));
                    count++;
                }
                Util.pause(emitStartTime);
            /*synchronized (ctx.getCheckpointLock()) {
                ctx.collect(Tuple2.of(getChar(count), System.currentTimeMillis()));
                count++;
            }
            if (count % (PHASE1_RATE * INTERVAL / 1000) == 0) {
                long ctime = System.currentTimeMillis();
                long nextTime = ((ctime - startTime) / INTERVAL + 1) * INTERVAL + startTime;
                if(nextTime >= ctime){
                    Thread.sleep(nextTime - ctime);
                }
            }*/
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
                long index = (System.currentTimeMillis() - startTime) / INTERVAL;
                if(remainedNumber <= 0){
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
                    ctx.collect(Tuple2.of(key, System.currentTimeMillis()));
                    // ctx.collect(Tuple2.of(getChar(count), System.currentTimeMillis()));
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

                    ctx.collect(Tuple2.of(key, System.currentTimeMillis()));
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
            return "A" + (cur % 1000);
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
