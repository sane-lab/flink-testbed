package flinkapp.StreamSluiceTestSet;

import Nexmark.sources.Util;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
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

public class TwoOpScaleOutTest {
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
        final int p2 = params.getInt("p2", 1);
        final int mp2 = params.getInt("mp2", 64);
        final double zipf_skew = params.getDouble("zipf_skew", 0);
        final int stateSize = params.getInt("state_size", 10);
        final long AVERAGE_LENGTH = params.getLong("sentence_length", 2);
        env.addSource(new TwoPhaseSineSource(PHASE1_TIME, PHASE2_TIME, INTERMEDIATE_TIME, PHASE1_RATE, PHASE2_RATE, INTERMEDIATE_RATE, INTERMEDIATE_PERIOD, mp1, zipf_skew, AVERAGE_LENGTH))
                .keyBy(0)
                .flatMap(new DumbSplitter())
                .disableChaining()
                .name("Splitter FlatMap")
                .uid("flatmap")
                .setMaxParallelism(mp1)
                .setParallelism(p1)
                .slotSharingGroup("a")
                .keyBy(0)
                .map(new DumbStatefulMap(stateSize))
                .name("Time counter")
                .setMaxParallelism(mp2)
                .setParallelism(p2)
                .slotSharingGroup("b");
        env.execute();
    }

    public static final class DumbSplitter implements FlatMapFunction<Tuple3<String, Long, Long>, Tuple3<String, Long, Long>> {

        private RandomDataGenerator randomGen = new RandomDataGenerator();
        @Override
        public void flatMap(Tuple3<String, Long, Long> input, Collector<Tuple3<String, Long, Long>> out) throws Exception {
            String[] tokens = input.f0.split("\\W+");
            for(String token: tokens) {
                long t = input.f1;
                long id = input.f2;
                out.collect(new Tuple3<String, Long, Long>(token, t, id));
            }
            delay(5);
        }

        private void delay(int interval) {
            Double ranN = randomGen.nextGaussian(interval, 0.1);
            ranN = ranN*1000000;
            long delay = ranN.intValue();
            if (delay < 0) delay = interval * 1000000;
            Long start = System.nanoTime();
            while (System.nanoTime() - start < delay) {}
        }
    }

    public static final class DumbStatefulMap extends RichMapFunction<Tuple3<String, Long, Long>, Tuple4<String, Long, Long, Long>> {

        private final int stateSize;
        private transient MapState<String, String> countMap;
        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private final String payload;
        DumbStatefulMap(int _stateSize){
            this.stateSize = _stateSize;
            this.payload = StringUtils.repeat("A", stateSize);
        }

        @Override
        public Tuple4<String, Long, Long, Long> map(Tuple3<String, Long, Long> input) throws Exception {
            countMap.put(input.f0, payload);
            delay(10);
            long currentTime = System.currentTimeMillis();
            System.out.println("GT: " + input.f0 + ", " + currentTime + ", " + (currentTime - input.f1) + ", " + input.f2);
            return new Tuple4<String, Long, Long, Long>(input.f0, currentTime, currentTime - input.f1, input.f2);
        }
        private void delay(int interval) {
            Double ranN = randomGen.nextGaussian(interval, 0.1);
            ranN = ranN*1000000;
            long delay = ranN.intValue();
            if (delay < 0) delay = interval * 1000000;
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
        private final long sentenceLength;


        public TwoPhaseSineSource(long PHASE1_TIME, long PHASE2_TIME, long INTERMEDIATE_TIME, long PHASE1_RATE, long PHASE2_RATE, long INTERMEDIATE_RATE, long INTERMEDIATE_PERIOD, int maxParallelism, double zipfSkew, long sentenceLength){
            this.PHASE1_TIME = PHASE1_TIME;
            this.PHASE2_TIME = PHASE2_TIME;
            this.INTERMEDIATE_TIME = INTERMEDIATE_TIME;
            this.PHASE1_RATE = PHASE1_RATE;
            this.PHASE2_RATE = PHASE2_RATE;
            this.INTERMEDIATE_RATE = INTERMEDIATE_RATE;
            this.INTERMEDIATE_PERIOD = INTERMEDIATE_PERIOD;
            this.INTERVAL = 50;
            this.sentenceLength = sentenceLength;
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
            // Phase 1
            long startTime = System.currentTimeMillis();
            System.out.println("Phase 1 start at: " + startTime);
            while (isRunning && System.currentTimeMillis() - startTime < PHASE1_TIME) {
                long emitStartTime = System.currentTimeMillis();
                for (int i = 0; i < PHASE1_RATE / 20; i++) {
                    String key = getSentence(count);//getChar(count);

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
                    String key = getSentence(count);//getChar(count);
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
                    String key = getSentence(count);//getChar(count);
                    ctx.collect(Tuple3.of(key, System.currentTimeMillis(), (long)count));
                    count++;
                }
                Util.pause(emitStartTime);
            }
        }

        private String getSentence(int cur){
            StringBuilder str = new StringBuilder();
            for(int i = 0; i < sentenceLength; i++) {
                String key = getChar(cur * sentenceLength + i);
                if(i > 0){
                    str.append(" ");
                }
                str.append(key);
            }
            return str.toString();
        }

        private String getChar(long cur) {
            return "A" + (cur % 1000);
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
