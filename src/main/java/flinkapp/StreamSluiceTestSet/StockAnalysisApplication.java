package flinkapp.StreamSluiceTestSet;

import Nexmark.sources.Util;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class StockAnalysisApplication {
    public static void main(String[] args) throws Exception {
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        env.setStateBackend(new MemoryStateBackend(1073741824));
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        DataStreamSource<Tuple6<String, String, Double, Double, Long, Long>> source = env.addSource(new SSERealRateSourceWithVolume(params.get("file_name", "/home/samza/SSE_data/sb-4hr-50ms.txt"), params.getLong("warmup_time", 30L) * 1000, params.getLong("warmup_rate", 1500L), params.getLong("skip_interval", 20L) * 20))
                .setParallelism(params.getInt("p1", 1));
        DataStream<Tuple5<String, Double, Double, Long, Long>> up = source
                .keyBy(0)
                .flatMap(new StockPreprocess(params.getInt("op2Delay", 1000)))
                .disableChaining()
                .name("Splitter")
                .uid("op2")
                .setParallelism(params.getInt("p2", 1))
                .setMaxParallelism(params.getInt("mp2", 8))
                .slotSharingGroup("g2");
        DataStream<Tuple5<String, Double, Integer, Long, Long>> split1 = up
                .keyBy(0)
                .flatMap(new PriceAnalysis(params.getInt("op3Delay", 4000)))
                .disableChaining()
                .name("Price Analysis")
                .uid("op3")
                .setParallelism(params.getInt("p3", 1))
                .setMaxParallelism(params.getInt("mp3", 8))
                .slotSharingGroup("g3");
        DataStream<Tuple5<String, Double, Integer, Long, Long>> split2 = up
                .keyBy(0)
                .flatMap(new VolumeFiltering(params.getInt("op4Delay", 1000)))
                .disableChaining()
                .name("Volume Filtering")
                .uid("op4")
                .setParallelism(params.getInt("p4", 1))
                .setMaxParallelism(params.getInt( "mp4", 8))
                .slotSharingGroup("g4")
                .keyBy(0)
                .flatMap(new VolumeAggregation(params.getInt("op5Delay", 2000)))
                .disableChaining()
                .name("Volume Aggregation")
                .uid("op5")
                .setParallelism(params.getInt("p5", 1))
                .setMaxParallelism(params.getInt( "mp5", 8))
                .slotSharingGroup("g5");
        DataStream<Tuple5<String, Double, Integer, Long, Long>> unionStream =  split1.union(split2);
        DataStream<Tuple5<String, Double, Double, Long, Long>> joined = unionStream.keyBy(0)
                .flatMap(new JoinMap())
                .disableChaining()
                .name("Join")
                .uid("op6")
                .setParallelism(params.getInt("p6", 1))
                .setMaxParallelism(params.getInt("mp6", 8))
                .slotSharingGroup("g6");

//        DataStream<Tuple5<String, Double, Double, Long, Long>> joined = ((SingleOutputStreamOperator)(split1
//                .join(split2)
//                .where((KeySelector<Tuple4<String, Double, Long, Long>, Long>) p -> p.f3)
//                .equalTo((KeySelector<Tuple4<String, Double, Long, Long>, Long>) p -> p.f3)
//                .window(TumblingEventTimeWindows.of(Time.milliseconds(1000)))
//                .apply(new JoinMap())))
//                .disableChaining()
//                .name("Join")
//                .uid("op6")
//                .setParallelism(params.getInt("p6", 1))
//                .setMaxParallelism(params.getInt("mp6", 8))
//                .slotSharingGroup("g6");

        joined.keyBy(0)
                .map(new Analysis(params.getInt("op7Delay", 3000)))
                .disableChaining()
                .name("Analysis")
                .uid("op7")
                .setParallelism(params.getInt("p7", 1))
                .setMaxParallelism(params.getInt("mp7", 8))
                .slotSharingGroup("g7");

        env.execute();
    }

    public static final class StockPreprocess extends RichFlatMapFunction<Tuple6<String, String, Double, Double, Long, Long>, Tuple5<String, Double, Double, Long, Long>> {
        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private int averageDelay; // Microsecond
        public StockPreprocess(int _averageDelay){
            this.averageDelay = _averageDelay;
        }

        @Override
        public void flatMap(Tuple6<String, String, Double, Double, Long, Long> input, Collector<Tuple5<String, Double, Double, Long, Long>> out) throws Exception {
            out.collect(new Tuple5<String, Double, Double, Long, Long>(input.f0, input.f2, input.f3, input.f4, input.f5));
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
    }

    public static final class PriceAnalysis extends RichFlatMapFunction<Tuple5<String, Double, Double, Long, Long>, Tuple5<String, Double, Integer, Long, Long>> {
        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private transient MapState<String, Double> lastPrice;
        private int averageDelay; // Microsecond
        public PriceAnalysis(int _averageDelay) {
            this.averageDelay = _averageDelay;
        }
        @Override
        public void flatMap(Tuple5<String, Double, Double, Long, Long> input, Collector<Tuple5<String, Double, Integer, Long, Long>> out) throws Exception {
            String s = input.f0;
            double difference;
            if(lastPrice.contains(s)){
                difference = input.f2 - lastPrice.get(s);
            }else{
                difference = 0;
            }
            lastPrice.put(s, input.f2);
            out.collect(new Tuple5<String, Double, Integer, Long, Long>(s, difference, 0, input.f3, input.f4));
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
            MapStateDescriptor<String, Double> descriptor =
                    new MapStateDescriptor<>("price-analysis", String.class, Double.class);
            lastPrice = getRuntimeContext().getMapState(descriptor);
        }
    }

    public static final class VolumeFiltering extends RichFlatMapFunction<Tuple5<String, Double, Double, Long, Long>, Tuple4<String, Double, Long, Long>> {

        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private final double volume_threshold;

        private int averageDelay; // Microsecond
        public VolumeFiltering(int _averageDelay) {
            this.averageDelay = _averageDelay;
            volume_threshold = 5.0;
        }

        @Override
        public void flatMap(Tuple5<String, Double, Double, Long, Long> input, Collector<Tuple4<String, Double, Long, Long>> out) throws Exception {
            String s = input.f0;
            if(input.f1 >= volume_threshold){
                out.collect(new Tuple4<String, Double, Long, Long>(s, input.f1, input.f3, input.f4));
            }else{
                out.collect(new Tuple4<String, Double, Long, Long>(s, 0.0, input.f3, input.f4));
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
    }

    public static final class VolumeAggregation extends RichFlatMapFunction<Tuple4<String, Double, Long, Long>, Tuple5<String, Double, Integer, Long, Long>> {

        private int averageDelay; // Microsecond
        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private transient MapState<String, Double> last1Volume, last2Volume, last3Volume, last4Volume, last5Volume;
        public VolumeAggregation(int _averageDelay) {
            averageDelay = _averageDelay;
        }

        @Override
        public void flatMap(Tuple4<String, Double, Long, Long> input, Collector<Tuple5<String, Double, Integer, Long, Long>> out) throws Exception {
            String s = input.f0;
            double totalVolume = 0;
            if(last5Volume.contains(s)){
                totalVolume += last5Volume.get(s);
                last5Volume.remove(s);
            }
            if(last4Volume.contains(s)){
                double volume = last4Volume.get(s);
                totalVolume += volume;
                last5Volume.put(s, volume);
                last4Volume.remove(s);
            }
            if(last3Volume.contains(s)){
                double volume = last3Volume.get(s);
                totalVolume += volume;
                last4Volume.put(s, volume);
                last3Volume.remove(s);
            }
            if(last2Volume.contains(s)){
                double volume = last2Volume.get(s);
                totalVolume += volume;
                last3Volume.put(s, volume);
                last2Volume.remove(s);
            }
            if(last1Volume.contains(s)){
                double volume = last1Volume.get(s);
                totalVolume += volume;
                last2Volume.put(s, volume);
                last1Volume.remove(s);
            }
            totalVolume += input.f1;
            last1Volume.put(s, input.f1);
            out.collect(new Tuple5<String, Double, Integer, Long, Long>(s, totalVolume, 1, input.f2, input.f3));
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
            MapStateDescriptor<String, Double> descriptor =
                    new MapStateDescriptor<>("volume-aggregation-1", String.class, Double.class);
            last1Volume = getRuntimeContext().getMapState(descriptor);
            descriptor = new MapStateDescriptor<>("volume-aggregation-2", String.class, Double.class);
            last2Volume = getRuntimeContext().getMapState(descriptor);
            descriptor = new MapStateDescriptor<>("volume-aggregation-3", String.class, Double.class);
            last3Volume = getRuntimeContext().getMapState(descriptor);
            descriptor = new MapStateDescriptor<>("volume-aggregation-4", String.class, Double.class);
            last4Volume = getRuntimeContext().getMapState(descriptor);
            descriptor = new MapStateDescriptor<>("volume-aggregation-5", String.class, Double.class);
            last5Volume = getRuntimeContext().getMapState(descriptor);
        }
    }

//    private static class JoinMap implements FlatJoinFunction<Tuple4<String, Double, Long, Long>, Tuple4<String, Double, Long, Long>, Tuple5<String, Double, Double, Long, Long>>{
//        private RandomDataGenerator randomGen = new RandomDataGenerator();
//        JoinMap(){
//        }
//        private void delay(long interval) {
//            Double ranN = randomGen.nextGaussian(interval, 1);
//            ranN = ranN*1000;
//            long delay = ranN.intValue();
//            if (delay < 0) delay = interval * 1000;
//            Long start = System.nanoTime();
//            while (System.nanoTime() - start < delay) {}
//        }
//        @Override
//        public void join(Tuple4<String, Double, Long, Long> split1_tuple, Tuple4<String, Double, Long, Long> split2_tuple, Collector<Tuple5<String, Double, Double, Long, Long>> collector) throws Exception {
//            if(Objects.equals(split1_tuple.f3, split2_tuple.f3)) {
//                collector.collect(new Tuple5<>(split1_tuple.f0, split1_tuple.f1, split2_tuple.f1, Math.min(split1_tuple.f2, split2_tuple.f2), split1_tuple.f3));
//            }
//            delay(100);
//        }
//    }


    private static class JoinMap extends RichFlatMapFunction<Tuple5<String, Double, Integer, Long, Long>, Tuple5<String, Double, Double, Long, Long>>{
        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private transient MapState<Long, Double> dataFromSplit1,  dataFromSplit2;
        private transient MapState<Long, Long> timeFromSplit1, timeFromSplit2;
        JoinMap(){
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
        public void flatMap(Tuple5<String, Double, Integer, Long, Long> input, Collector<Tuple5<String, Double, Double, Long, Long>> out) throws Exception {
            String s = input.f0;
            int type = input.f2;
            if (type == 0){ // From Split 1
                 if(dataFromSplit2.contains(input.f4)){
                     double value = dataFromSplit2.get(input.f4);
                     long time = timeFromSplit2.get(input.f4);
                     out.collect(new Tuple5<>(s, input.f1, value, Math.min(input.f3, time), input.f4));
                     dataFromSplit2.remove(input.f4);
                     timeFromSplit2.remove(input.f4);
                 }else{
                     dataFromSplit1.put(input.f4, input.f1);
                     timeFromSplit1.put(input.f4, input.f3);
                 }
            }else{ // From Split 2
                if(dataFromSplit1.contains(input.f4)){
                    double value = dataFromSplit1.get(input.f4);
                    long time = timeFromSplit1.get(input.f4);
                    out.collect(new Tuple5<>(s, value, input.f1, Math.min(input.f3, time), input.f4));
                    dataFromSplit1.remove(input.f4);
                    timeFromSplit1.remove(input.f4);
                }else{
                    dataFromSplit2.put(input.f4, input.f1);
                    timeFromSplit2.put(input.f4, input.f3);
                }
            }
            delay(100);
        }
        @Override
        public void open(Configuration config) {
            MapStateDescriptor<Long, Double> descriptor =
                    new MapStateDescriptor<>("join-map-1", Long.class, Double.class);
            dataFromSplit1 = getRuntimeContext().getMapState(descriptor);
            descriptor = new MapStateDescriptor<>("join-map-2", Long.class, Double.class);
            dataFromSplit2 = getRuntimeContext().getMapState(descriptor);
            MapStateDescriptor<Long, Long> descriptor1 =
                    new MapStateDescriptor<>("join-map-3", Long.class, Long.class);
            timeFromSplit1 = getRuntimeContext().getMapState(descriptor1);
            descriptor1 = new MapStateDescriptor<>("join-map-4", Long.class, Long.class);
            timeFromSplit2 = getRuntimeContext().getMapState(descriptor1);
        }
    }

    public static final class Analysis extends RichMapFunction<Tuple5<String, Double, Double, Long, Long>, Tuple4<String, String, Long, Long>> {

        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private int averageDelay; // Microsecond
        Analysis(int _averageDelay){
            this.averageDelay = _averageDelay;
        }
        @Override
        public Tuple4<String, String, Long, Long> map(Tuple5<String, Double, Double, Long, Long> input) throws Exception {
            delay(averageDelay);
            String result;
            if(input.f1 > 0 && input.f2 > 100){
                result = "anomaly detected in " + input.f0;
            }else{
                result = "";
            }
            long currentTime = System.currentTimeMillis();
            System.out.println("GT: " + input.f0 + ", " + currentTime + ", " + (currentTime - input.f3) + ", " + input.f4);
            return new Tuple4<String, String, Long, Long>(input.f0, result, currentTime - input.f3, input.f4);
        }
        private void delay(long interval) {
            Double ranN = randomGen.nextGaussian(interval, 1);
            ranN = ranN*1000;
            long delay = ranN.intValue();
            if (delay < 0) delay = interval * 1000;
            Long start = System.nanoTime();
            while (System.nanoTime() - start < delay) {}
        }
    }

    private static class SSERealRateSourceWithVolume extends RichParallelSourceFunction<Tuple6<String, String, Double, Double, Long, Long>> {

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

        public SSERealRateSourceWithVolume(String FILE, long warmup, long warmup_rate, long skipCount) {
            this.FILE = FILE;
            this.warmup = warmup;
            this.warmp_rate = warmup_rate;
            this.skipCount = skipCount;
        }

        @Override
        public void run(SourceContext<Tuple6<String, String, Double, Double, Long, Long>> ctx) throws Exception {
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
                    ctx.collect(Tuple6.of(key, "A", 0.0, 0.0, System.currentTimeMillis(), (long)count));
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
                                ctx.collect(Tuple6.of(key, "A", 0.0, 0.0, System.currentTimeMillis(), (long)count));
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
                        ctx.collect(new Tuple6<>(stockArr.get(Sec_Code), stockArr.get(Sec_Code + 2), Double.parseDouble(stockArr.get(Order_Vol)), Double.parseDouble(stockArr.get(Order_Price)), ts, (long) count));
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
