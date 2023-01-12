package flinkapp;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.io.FileOutputStream;
import java.util.Random;

public class MultiStageLatency {
    //    private static final int MAX = 1000000 * 10;
    private static final int MAX = 10000;
    private static final int NUM_LETTERS = 1000;
    // private static final long TOTAL = 50000;
    // private static final long WARMUP_TIME = 30000;
    // private static final long RATE = 400;
    // private static final long PERIOD = 20000;
    // private static final long AMPLITUDE = 300;
    // private static final long INTERVAL = 50;

    public static void main(String[] args) throws Exception {
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        env.setStateBackend(new MemoryStateBackend(100000000));
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

//        FlinkKafkaProducer011<String> kafkaProducer = new FlinkKafkaProducer011<String>(
//                "localhost:9092", "my-flink-demo-topic0", new SimpleStringSchema());
//        kafkaProducer.setWriteTimestampToKafka(true);
        final long RUN_TIME = params.getLong("runTime", 720) * 1000;
        final long WARMUP_TIME = params.getLong("srcWarmUp", 30) * 1000;
        final long WARMUP_RATE = params.getLong("srcWarmupRate", 300);
        final long RATE = params.getLong("srcRate", 500);
        final long PERIOD = params.getLong("srcPeriod", 60) * 1000;
        final long AMPLITUDE = params.getLong("srcAmplitude", 300);
        final long INTERVAL = params.getLong("srcInterval", 50);
        final int p1 = params.getInt("p1", 1);
        final int p2 = params.getInt("p2", 2);
        final int mp1 = params.getInt("mp1", 64);
        final int mp2 = params.getInt("mp2", 64);

        env.addSource(new MySource(RUN_TIME, WARMUP_TIME, WARMUP_RATE, RATE, PERIOD, AMPLITUDE, INTERVAL))
                .keyBy(0)
                .map(new MyStatefulMap())
                .disableChaining()
                .name("Splitter FlatMap")
                .uid("flatmap")
                .setMaxParallelism(mp1)
                .setParallelism(p1)
                .slotSharingGroup("a")
                .keyBy(0)
                .map(new Tokenizer())
                .disableChaining()
                .name("Time counter")
                .setMaxParallelism(mp2)
                .setParallelism(p2)
                .slotSharingGroup("b");
                //.keyBy(0)
                //.map(new DumbMap())
                //.keyBy(0)
                //.sum(1)
                //.print();
//
//            .addSink(kafkaProducer);
        env.execute();
    }

    private static class MyStatefulMap extends RichMapFunction<Tuple3<String, String, Long>, Tuple2<String, Long>> {

        private transient MapState<String, Long> countMap;
        private transient FileOutputStream outputStream;
        private RandomDataGenerator randomGen = new RandomDataGenerator();

        @Override
        public Tuple2<String, Long> map(Tuple3<String, String, Long> input) throws Exception {
            String s = input.f0;
            long t = input.f2;
            Long cur = countMap.get(s);
            cur = (cur == null) ? 1 : cur + 1;
            countMap.put(s, cur);
            // long currTime = System.currentTimeMillis();
//            outputStream.write(
//                    String.format("current time in ms: %d, queueing delay + processing delay in ms: %d\n",
//                            currTime, currTime - input.f2).getBytes()
//            );
            delay(1);
            return new Tuple2<String, Long>(String.format("%s %d", s, cur), t);
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
            MapStateDescriptor<String, Long> descriptor =
                    new MapStateDescriptor<>("word-count", String.class, Long.class);
//            try {
//                if(outputStream == null) {
//                    outputStream = new FileOutputStream("/home/hya/prog/latency.out");
//                }else {
//                    System.out.println("already have an ouput stream during last open");
//                }
//            } catch (FileNotFoundException e) {
//                e.printStackTrace();
//            }
            countMap = getRuntimeContext().getMapState(descriptor);
        }
    }

    private static class Tokenizer implements MapFunction<Tuple2<String, Long>, Tuple3<String, Long, Long>> {
        private RandomDataGenerator randomGen = new RandomDataGenerator();
        @Override
        public Tuple3<String, Long, Long> map(Tuple2<String, Long> input) throws Exception {
            delay(2);
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
    }

    private static class DumbMap implements MapFunction<Tuple2<String, Long>, Tuple3<Long, Long, Long>> {
        @Override
        public Tuple3<Long, Long, Long> map(Tuple2<String, Long> input) throws Exception {
            return new Tuple3<Long, Long, Long>(1l, 1l, input.f1);
        }
    }

    private static class MySource implements SourceFunction<Tuple3<String, String, Long>>, CheckpointedFunction {
        private long RUN_TIME, WARMUP_TIME, WARMUP_RATE, RATE, PERIOD, AMPLITUDE, INTERVAL;

        private int count = 0;
        private volatile boolean isRunning = true;

        private transient ListState<Integer> checkpointedCount;

        public MySource(long RUN_TIME, long WARMUP_TIME, long WARMUP_RATE, long RATE, long PERIOD, long AMPLITUDE, long INTERVAL){
            this.RUN_TIME = RUN_TIME;
            this.WARMUP_TIME = WARMUP_TIME;
            this.WARMUP_RATE = WARMUP_RATE;
            this.RATE = RATE;
            this.PERIOD = PERIOD;
            this.AMPLITUDE = AMPLITUDE;
            this.INTERVAL = INTERVAL;
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

        @Override
        public void run(SourceContext<Tuple3<String, String, Long>> ctx) throws Exception {
            long remainedNumber = 0;
            long startTime = System.currentTimeMillis();
            while (isRunning && System.currentTimeMillis() - startTime < WARMUP_TIME) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(Tuple3.of(getChar(count), getChar(count), System.currentTimeMillis()));
                    count++;
                }
                if (count % (WARMUP_RATE * INTERVAL / 1000) == 0) {
                    long ctime = System.currentTimeMillis();
                    long nextTime = ((ctime- startTime) / INTERVAL + 1) * INTERVAL + startTime;
                    Thread.sleep(nextTime - ctime);
                }
            }

            if (!isRunning) {
                return ;
            }
            startTime = System.currentTimeMillis();
            System.out.println("Warmup end at: " + startTime);
            remainedNumber = (long)Math.floor(RATE * INTERVAL / 1000.0);
            long index = 0;
            while (isRunning && System.currentTimeMillis() - startTime < RUN_TIME) {
                if(remainedNumber == 0){
                    index++;
                    long ntime = index * INTERVAL + startTime;
                    double theta = Math.sin(Math.toRadians(index * INTERVAL * 360 / ((double)PERIOD)));
                    remainedNumber = (long)Math.floor((RATE + theta * AMPLITUDE) / 1000 * INTERVAL);
                    Thread.sleep(ntime - System.currentTimeMillis());
                }
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(Tuple3.of(getChar(count), getChar(count), System.currentTimeMillis()));
                    remainedNumber --;
                    count++;
                }

            }
        }

        private static String getChar(int cur) {
//            return String.valueOf((char) ('A' + (cur % NUM_LETTERS)));
            return "A" + (cur % NUM_LETTERS);
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
