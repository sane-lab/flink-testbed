package flinkapp.StreamSluiceTestSet;

import Nexmark.sources.Util;
import common.FastZipfGenerator;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
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
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MathUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class StockTest {
    public static void main(String[] args) throws Exception {
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        env.setStateBackend(new MemoryStateBackend(1073741824));
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        String topology = params.get("topology", "3op");
        if(topology.equals("3op")) {
            env.addSource(new SSERealRateSource(params.get("file_name", "/home/samza/SSE_data/sb-4hr-50ms.txt"), params.getLong("warmup_time", 30L) * 1000, params.getLong("warmup_rate", 1500L), params.getLong("skip_interval", 20L) * 20))
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
                    .map(new DumbSink(params.getLong("op4Delay", 100), params.getInt("op4KeyStateSize", 1)))
                    .disableChaining()
                    .name("FlatMap 4")
                    .uid("op4")
                    .setParallelism(params.getInt("p4", 1))
                    .setMaxParallelism(params.getInt("mp4", 8))
                    .slotSharingGroup("g4");
        }else if(topology.equals("split")){
            DataStreamSource<Tuple3<String, Long, Long>> source = env.addSource(new SSERealRateSource(params.get("file_name", "/home/samza/SSE_data/sb-4hr-50ms.txt"), params.getLong("warmup_time", 30L) * 1000, params.getLong("warmup_rate", 1500L), params.getLong("skip_interval", 20L) * 20))
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
            up.keyBy(0).map(new DumbSink(params.getLong("op3Delay", 100), params.getInt("op3KeyStateSize", 1)))
                    .disableChaining()
                    .name("Analytic 1")
                    .uid("op3")
                    .setParallelism(params.getInt("p3", 1))
                    .setMaxParallelism(params.getInt("mp3", 8))
                    .slotSharingGroup("g3");

            up.keyBy(0).map(new DumbSink(params.getLong("op4Delay", 100), params.getInt("op4KeyStateSize", 1)))
                    .disableChaining()
                    .name("Analytic 2")
                    .uid("op4")
                    .setParallelism(params.getInt("p4", 1))
                    .setMaxParallelism(params.getInt("mp4", 8))
                    .slotSharingGroup("g4");

        }else if(topology.equals("split_join")){
            env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
            DataStreamSource<Tuple3<String, Long, Long>> source = env.addSource(new SSERealRateSource(params.get("file_name", "/home/samza/SSE_data/sb-4hr-50ms.txt"), params.getLong("warmup_time", 30L) * 1000, params.getLong("warmup_rate", 1500L), params.getLong("skip_interval", 20L) * 20))
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
            DataStream<Tuple3<String, Long, Long>> split1 = up.keyBy(0).flatMap(new DumbStatefulMap(params.getLong("op3Delay", 100), params.getInt("op3IoRate", 1), params.getInt("op3KeyStateSize", 1)))
                    .disableChaining()
                    .name("Analytic 1")
                    .uid("op3")
                    .setParallelism(params.getInt("p3", 1))
                    .setMaxParallelism(params.getInt("mp3", 8))
                    .slotSharingGroup("g3");
            DataStream<Tuple3<String, Long, Long>> split2 = up.keyBy(0).flatMap(new DumbStatefulMap(params.getLong("op4Delay", 100), params.getInt("op4IoRate", 1), params.getInt("op4KeyStateSize", 1)))
                    .disableChaining()
                    .name("Analytic 2")
                    .uid("op4")
                    .setParallelism(params.getInt("p4", 1))
                    .setMaxParallelism(params.getInt( "mp4", 8))
                    .slotSharingGroup("g4");
            DataStream<Tuple3<String, Long, Long>> unionStream =  split1.union(split2);
            unionStream.keyBy(0)
                    .map(new DumbSink(params.getLong("op5Delay", 100), params.getInt("op5KeyStateSize", 1)))
                    .disableChaining()
                    .name("Join")
                    .uid("op5")
                    .setParallelism(params.getInt("p5", 1))
                    .setMaxParallelism(params.getInt("mp5", 8))
                    .slotSharingGroup("g5");
//            ((SingleOutputStreamOperator)(split1
//                    .join(split2)
//                    .where((KeySelector<Tuple3<String, Long, Long>, Long>) p -> p.f2)
//                    .equalTo((KeySelector<Tuple3<String, Long, Long>, Long>) p -> p.f2)
//                    .window(TumblingEventTimeWindows.of(Time.milliseconds(params.getInt("op5window", 1000))))
//                    .apply(
//                            new DumbJoinMap(params.getInt("op5Delay", 100))
//                    ))).disableChaining()
//                    .name("Join")
//                    .uid("op5")
//                    .setParallelism(params.getInt("p5", 1))
//                    .setMaxParallelism(params.getInt("mp5", 8))
//                    .slotSharingGroup("g5");
        }
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

    private static class DumbJoinMap implements FlatJoinFunction<Tuple3<String, Long, Long>, Tuple3<String, Long, Long>, Tuple3<String, Long, Long>>{
        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private long averageDelay = 1000; // micro second
        DumbJoinMap(long averageDelay){
            this.averageDelay = averageDelay;
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
        public void join(Tuple3<String, Long, Long> split1_tuple, Tuple3<String, Long, Long> split2_tuple, Collector<Tuple3<String, Long, Long>> collector) throws Exception {
            delay(averageDelay);
            long currentTime = System.currentTimeMillis();
            // System.out.println("join: t1=" + split1_tuple.toString() + " t2=" + split2_tuple.toString());
            if(Objects.equals(split1_tuple.f2, split2_tuple.f2)) {
                System.out.println("GT: " + split1_tuple.f0 + ", " + currentTime + ", " + (currentTime - Math.min(split1_tuple.f1, split2_tuple.f1)) + ", " + split1_tuple.f2);
                collector.collect(new Tuple3<>(split1_tuple.f0, Math.min(split1_tuple.f1, split2_tuple.f1), split1_tuple.f2));
            }
        }
    }


    private static class SSERealRateSource extends RichParallelSourceFunction<Tuple3<String, Long, Long>> {

        private volatile boolean running = true;


        private static final int Order_No = 0;
        private static final int Tran_Maint_Code = 1;
        private static final int Order_Price = 8;
        private static final int Order_Exec_Vol = 9;
        private static final int Order_Vol = 10;
        private static final int Sec_Code = 11;
        private static final int Trade_Dir = 22;

        private final String FILE;
        private final long warmup, warmp_rate, skipCount;

        public SSERealRateSource(String FILE, long warmup, long warmup_rate, long skipCount) {
            this.FILE = FILE;
            this.warmup = warmup;
            this.warmp_rate = warmup_rate;
            this.skipCount = skipCount;
        }

        @Override
        public void run(SourceContext<Tuple3<String, Long, Long>> ctx) throws Exception {
            String sCurrentLine;
            List<String> textList = new ArrayList<>();
            FileReader stream = null;
            // // for loop to generate message
            BufferedReader br = null;
            int sent_sentences = 0;
            long cur = 0;
            long start = 0;
            int counter = 0, count = 0;

            int noRecSleepCnt = 0;
            int sleepCnt = 0;

            long startTime = System.currentTimeMillis();
            System.out.println("Warmup start at: " + startTime);
            while (System.currentTimeMillis() - startTime < warmup) {
                long emitStartTime = System.currentTimeMillis();
                for (int i = 0; i < warmp_rate / 20; i++) {
                    String key = "A" + (count % 10000);
                    ctx.collect(Tuple3.of(key, System.currentTimeMillis(), (long)count));
                    count++;
                }
                Util.pause(emitStartTime);
            }
//        Thread.sleep(60000);

            try {
                stream = new FileReader(FILE);
                br = new BufferedReader(stream);

                start = System.currentTimeMillis();

                while ((sCurrentLine = br.readLine()) != null) {
                    if (sCurrentLine.equals("end")) {
                        sleepCnt++;
                        if (counter == 0) {
                            noRecSleepCnt++;
                            System.out.println("no record in this sleep !" + noRecSleepCnt);
                        }
                        // System.out.println("output rate: " + counter);
                        if(sleepCnt <= skipCount){
                            for (int i = 0; i < warmp_rate / 20; i++) {
                                String key = "A" + (count % 10000);
                                ctx.collect(Tuple3.of(key, System.currentTimeMillis(), (long)count));
                                count++;
                            }
                        }
                        counter = 0;
                        cur = System.currentTimeMillis();
                        if (cur < sleepCnt * 50 + start) {
                            Thread.sleep((sleepCnt * 50 + start) - cur);
                        } else {
                            System.out.println("rate exceeds" + 50 + "ms.");
                        }
//                    start = System.currentTimeMillis();
                    }

                    if (sCurrentLine.split("\\|").length < 10) {
                        continue;
                    }

                    if(sleepCnt > skipCount) {
                        Long ts = System.currentTimeMillis();
                        String msg = sCurrentLine;
                        List<String> stockArr = Arrays.asList(msg.split("\\|"));
                        ctx.collect(new Tuple3<>(stockArr.get(Sec_Code), ts, (long) count));
                        count++;
                    }
                    counter++;
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if(stream != null) stream.close();
                    if(br != null) br.close();
                } catch(IOException ex) {
                    ex.printStackTrace();
                }
            }

            ctx.close();
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
