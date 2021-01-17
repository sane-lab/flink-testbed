package flinkapp;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;

public class StatefulDemo {

    //    private static final int MAX = 1000000 * 10;
    private static final int MAX = 1000000 * 4;
    private static final int NUM_LETTERS = 26;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

//        FlinkKafkaProducer011<String> kafkaProducer = new FlinkKafkaProducer011<String>(
//                "localhost:9092", "my-flink-demo-topic0", new SimpleStringSchema());
//        kafkaProducer.setWriteTimestampToKafka(true);

        env.addSource(new MySource())
                .keyBy(0)
                .map(new MyStatefulMap())
                .disableChaining()
                .name("Splitter FlatMap")
                .uid("flatmap")
                .setParallelism(2)
                .keyBy((KeySelector<String, Object>) s -> s)
                .filter(input -> {
                    return Integer.parseInt(input.split(" ")[1]) >= MAX;
                })
                .name("filter")
                .print();
//            .addSink(kafkaProducer);
        env.execute();
    }

    private static class MyStatefulMap extends RichMapFunction<Tuple3<String, String, Long>, String> {

        private transient MapState<String, Long> countMap;
        private transient FileOutputStream outputStream;

        @Override
        public String map(Tuple3<String, String, Long> input) throws Exception {
            String s = input.f0;

            Long cur = countMap.get(s);
            cur = (cur == null) ? 1 : cur + 1;
            countMap.put(s, cur);
            long currTime = System.currentTimeMillis();
            outputStream.write(
                    String.format("current time in ms: %d, queueing delay + processing delay in ms: %d\n",
                            currTime, currTime - input.f2).getBytes()
            );
            return String.format("%s %d", s, cur);
        }

        @Override
        public void open(Configuration config) {
            MapStateDescriptor<String, Long> descriptor =
                    new MapStateDescriptor<>("word-count", String.class, Long.class);
            try {
                if(outputStream == null) {
                    outputStream = new FileOutputStream("/home/hya/prog/latency.out");
                }else {
                    System.out.println("already have an ouput stream during last open");
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            countMap = getRuntimeContext().getMapState(descriptor);
        }
    }

    private static class MySource implements SourceFunction<Tuple3<String, String, Long>>, CheckpointedFunction {

        private int count = 0;
        private volatile boolean isRunning = true;

        private transient ListState<Integer> checkpointedCount;

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
            while (isRunning && count < NUM_LETTERS * MAX) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(Tuple3.of(getChar(count), getChar(count), System.currentTimeMillis()));
                    count++;
                }
//                Thread.sleep(1000);
            }
        }

        private static String getChar(int cur) {
            return String.valueOf((char) ('A' + (cur % NUM_LETTERS)));
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
