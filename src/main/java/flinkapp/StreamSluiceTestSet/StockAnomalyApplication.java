package flinkapp.StreamSluiceTestSet;

import Nexmark.sources.Util;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Collector;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class StockAnomalyApplication {
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
                .name("Preprocess")
                .uid("op2")
                .setParallelism(params.getInt("p2", 1))
                .setMaxParallelism(params.getInt("mp2", 8))
                .slotSharingGroup("g2");
        DataStream<Tuple6<String, Double, Double, Integer, Long, Long>> up1 = up
                .keyBy(0)
                .flatMap(new StockFilter(params.getInt("op3Delay", 4000), 2.0))
                .disableChaining()
                .name("Filter")
                .uid("op3")
                .setParallelism(params.getInt("p3", 1))
                .setMaxParallelism(params.getInt("mp3", 8))
                .slotSharingGroup("g3");

        DataStream<Tuple6<String, Double, Double, Integer, Long, Long>> staticSource = env.addSource(new HistoryAverageSource(params.get("file_name", "/home/samza/SSE_data/sb-history-average.txt"), params.getLong("warmup_time", 30L) * 1000, params.getLong("warmup_rate", 1500L), params.getLong("history_interval", 60L) * 1000))
                .setParallelism(params.getInt("p4", 1));
        DataStream<Tuple6<String, Double, Double, Integer, Long, Long>> unionStream =  up1.union(staticSource);

        DataStream<Tuple6<String, Double, Double, Double, Long, Long>> joined = unionStream.keyBy(0)
                .flatMap(new Enrich())
                .disableChaining()
                .name("Enrich")
                .uid("op5")
                .setParallelism(params.getInt("p5", 1))
                .setMaxParallelism(params.getInt("mp5", 8))
                .slotSharingGroup("g5");

        DataStream<Tuple6<String, Double, Double, Double, Long, Long>> aggregated = joined.keyBy(0)
                .flatMap(new Aggregation(params.getInt("op6Delay", 3000)))
                .disableChaining()
                .name("Aggregation")
                .uid("op6")
                .setParallelism(params.getInt("p6", 1))
                .setMaxParallelism(params.getInt("mp6", 8))
                .slotSharingGroup("g6");

        aggregated.keyBy(0)
                .map(new Detection(params.getInt("op7Delay", 3000)))
                .disableChaining()
                .name("Anomaly Detection")
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

    public static final class StockFilter extends RichFlatMapFunction<Tuple5<String, Double, Double, Long, Long>, Tuple6<String, Double, Double, Integer, Long, Long>> {
        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private int averageDelay; // Microsecond
        private double volumeBar; // Microsecond
        public StockFilter(int _averageDelay, double _volumeBar) {
            this.averageDelay = _averageDelay;
            this.volumeBar = _volumeBar;
        }
        @Override
        public void flatMap(Tuple5<String, Double, Double, Long, Long> input, Collector<Tuple6<String, Double, Double, Integer, Long, Long>> out) throws Exception {
            String s = input.f0;
            if(input.f2 > volumeBar){
                out.collect(new Tuple6<String, Double, Double, Integer, Long, Long>(s, input.f1, input.f2, 0, input.f3, input.f4));
            }else{
                long currentTime = System.currentTimeMillis();
                System.out.println("GT: " + input.f0 + ", " + currentTime + ", " + (currentTime - input.f3) + ", " + input.f4);
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

    private static class Enrich extends RichFlatMapFunction<Tuple6<String, Double, Double, Integer, Long, Long>, Tuple6<String, Double, Double, Double, Long, Long>>{
        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private transient MapState<String, Double> dataFromEnrichSource;
        Enrich(){
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
        public void flatMap(Tuple6<String, Double, Double, Integer, Long, Long> input, Collector<Tuple6<String, Double, Double, Double, Long, Long>> out) throws Exception {
            String s = input.f0;
            int type = input.f3;
            if (type == 0){ // From Split 1
                if(dataFromEnrichSource.contains(s)){
                    double history_average = dataFromEnrichSource.get(s);
                    out.collect(new Tuple6<>(s, input.f1, input.f2, history_average, input.f4, input.f5));
                }else{
                    double history_average = -1.0;
                    out.collect(new Tuple6<>(s, input.f1, input.f2, history_average, input.f4, input.f5));
                }
            }else{ // Enrich data
                dataFromEnrichSource.put(s, input.f1);
                long currentTime = System.currentTimeMillis();
                System.out.println("GT1: " + s + ", " + currentTime + ", " + (currentTime - input.f4) + ", " + input.f5);
            }
            delay(100);
        }
        @Override
        public void open(Configuration config) {
            MapStateDescriptor<String, Double> descriptor =
                    new MapStateDescriptor<>("enrich", String.class, Double.class);
            dataFromEnrichSource = getRuntimeContext().getMapState(descriptor);
        }
    }

    public static final class Aggregation extends RichFlatMapFunction<Tuple6<String, Double, Double, Double, Long, Long>, Tuple6<String, Double, Double, Double, Long, Long>> {

        private int averageDelay; // Microsecond
        private long windowLength;
        private transient long currentWindowIndex;
        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private transient MapState<String, Double> totalVolume, totalVPrice, historyPrice;
        private transient MapState<String, Long> arriveTime, arriveNum;
        public Aggregation(int _averageDelay) {
            averageDelay = _averageDelay;
            windowLength = 500;
            currentWindowIndex = -1;
        }

        @Override
        public void flatMap(Tuple6<String, Double, Double, Double, Long, Long> input, Collector<Tuple6<String, Double, Double, Double, Long, Long>> out) throws Exception {
            String s = input.f0;
            long ctime = System.currentTimeMillis();
            long index = ctime / windowLength;
            if(currentWindowIndex == - 1){
                currentWindowIndex = index;
            }
            if(index > currentWindowIndex){
                for(String key: totalVolume.keys()){
                    double volume = totalVolume.get(key), averagePrice = totalVPrice.get(key)/volume, history = historyPrice.get(key);
                    out.collect(new Tuple6<String, Double, Double, Double, Long, Long>(s, volume, averagePrice, history, arriveTime.get(key), arriveNum.get(key)));
                }
                totalVolume.clear();
                totalVPrice.clear();
                historyPrice.clear();
                arriveTime.clear();
                arriveNum.clear();
                currentWindowIndex = index;
            }
            if(totalVolume.contains(s)){
                double volume = totalVolume.get(s), totalPrice = totalVPrice.get(s);
                totalVolume.put(s, volume + input.f2);
                totalVPrice.put(s, totalPrice + volume * input.f1);
                historyPrice.put(s, input.f3);
                arriveTime.put(s, input.f4);
                arriveNum.put(s, input.f5);
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
            MapStateDescriptor<String, Double> descriptor =
                    new MapStateDescriptor<>("aggregation-volume", String.class, Double.class);
            totalVolume = getRuntimeContext().getMapState(descriptor);
            descriptor = new MapStateDescriptor<>("aggregation-price", String.class, Double.class);
            totalVPrice = getRuntimeContext().getMapState(descriptor);
            descriptor = new MapStateDescriptor<>("aggregation-historical", String.class, Double.class);
            historyPrice = getRuntimeContext().getMapState(descriptor);
            MapStateDescriptor<String, Long> descriptor1 = new MapStateDescriptor<>("aggregation-arriveTime", String.class, Long.class);
            arriveTime = getRuntimeContext().getMapState(descriptor1);
            descriptor1 = new MapStateDescriptor<>("aggregation-arriveNum", String.class, Long.class);
            arriveNum = getRuntimeContext().getMapState(descriptor1);
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




    public static final class Detection extends RichMapFunction<Tuple6<String, Double, Double, Double, Long, Long>, Tuple4<String, String, Long, Long>> {

        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private int averageDelay; // Microsecond
        Detection(int _averageDelay){
            this.averageDelay = _averageDelay;
        }
        @Override
        public Tuple4<String, String, Long, Long> map(Tuple6<String, Double, Double, Double, Long, Long> input) throws Exception {
            delay(averageDelay);
            String result;
            if(input.f2 > 10.0 && Math.abs(input.f1 - input.f3) > input.f3 * 0.20){
                result = "anomaly detected in " + input.f0;
            }else{
                result = "";
            }
            long currentTime = System.currentTimeMillis();
            System.out.println("GT: " + input.f0 + ", " + currentTime + ", " + (currentTime - input.f4) + ", " + input.f5);
            return new Tuple4<String, String, Long, Long>(input.f0, result, currentTime - input.f4, input.f5);
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

    private static class HistoryAverageSource extends RichParallelSourceFunction<Tuple6<String, Double, Double, Integer, Long, Long>> {

        private volatile boolean running = true;


        private static final int Order_No = 0;
        private static final int Tran_Maint_Code = 1;
        private static final int Order_Price = 8;
        private static final int Order_Exec_Vol = 9;
        private static final int Order_Vol = 10;
        private static final int Sec_Code = 11;
        private static final int Trade_Dir = 22;

        private final String FILE;
        private final long warmup, warmp_rate, interval;

        public HistoryAverageSource(String FILE, long warmup, long warmup_rate, long interval) {
            this.FILE = FILE;
            this.warmup = warmup;
            this.warmp_rate = warmup_rate;
            this.interval = interval;
        }

        @Override
        public void run(SourceContext<Tuple6<String, Double, Double, Integer, Long, Long>> ctx) throws Exception {
            String sCurrentLine;
            List<String> textList = new ArrayList<>();
            FileReader stream = null;
            // // for loop to generate message
            BufferedReader br = null;
            int sent_sentences = 0;
            long cur = 0;
            long start = 0;
            int count = 0;

            long startTime = System.currentTimeMillis();
            System.out.println("Warmup start at: " + startTime);
            while (System.currentTimeMillis() - startTime < warmup) {
                long emitStartTime = System.currentTimeMillis();
                for (int i = 0; i < warmp_rate / 20; i++) {
                    String key = "A" + (count % 10000);
                    ctx.collect(Tuple6.of(key, 0.0, 0.0, 1, System.currentTimeMillis(), (long)count));
                    count++;
                }
                Util.pause(emitStartTime);
            }

            try {
                while(running) {
                    stream = new FileReader(FILE);
                    br = new BufferedReader(stream);

                    start = System.currentTimeMillis();
                    while ((sCurrentLine = br.readLine()) != null) {
                        if (sCurrentLine.split("\\|").length < 10) {
                            continue;
                        }
                        Long ts = System.currentTimeMillis();
                        String msg = sCurrentLine;
                        List<String> stockArr = Arrays.asList(msg.split("\\|"));
                        ctx.collect(new Tuple6<>(stockArr.get(Sec_Code), Double.parseDouble(stockArr.get(Order_Vol)), Double.parseDouble(stockArr.get(Order_Price)), 0, ts, (long) count));
                        count++;
                    }
                    Thread.sleep(interval);
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
