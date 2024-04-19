package flinkapp.linearroad;

import Nexmark.sources.Util;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.*;
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

public class LinearRoad {
    private static final int Source_Output = 0;
    private static final int AccidentDetection_Output = 1;
    private static final int AverageSpeed_Output = 3;
    private static final int LastAverageSpeed_Output = 4;
    private static final int CountVehicles_Output = 5;
    private static final int TollNotification_Output = 6;
    private static final int AccountBalance_Output = 7;
    public static void main(String[] args) throws Exception {
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        env.setStateBackend(new MemoryStateBackend(1073741824));
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        DataStreamSource<Tuple18<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> source =
                env.addSource(new LinearRoadSource(params.get("file_name", "/home/samza/LR_data/3hr.txt"),
                                params.getLong("warmup_time", 30L) * 1000,
                                params.getLong("warmup_rate", 1500L),
                                params.getLong("skip_interval", 0L) * 20))
                        .setParallelism(params.getInt("p1", 1));

        DataStream<Tuple18<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> afterAccidentDetection = source
                .keyBy(LinearRoadSource.Car_ID)
                .flatMap(new AccidentDetection(params.getInt("op2Delay", 1000)))
                .disableChaining()
                .name("Accident Detection")
                .uid("op2")
                .setParallelism(params.getInt("p2", 1))
                .setMaxParallelism(params.getInt("mp2", 8))
                .slotSharingGroup("g2");
        afterAccidentDetection.union(source)
                .keyBy(LinearRoadSource.Seg)
                .map(new AccidentNotification(params.getInt("op3Delay", 1000)))
                .disableChaining()
                .name("Accident Notification")
                .uid("op3")
                .setParallelism(params.getInt("p3", 1))
                .setMaxParallelism(params.getInt("mp3", 8))
                .slotSharingGroup("g3");

        DataStream<Tuple18<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> afterAverageSpeed = source
                .keyBy(LinearRoadSource.Seg)
                .flatMap(new AverageSpeed(params.getInt("op4Delay", 1000)))
                .disableChaining()
                .name("Average Speed")
                .uid("op4")
                .setParallelism(params.getInt("p4", 1))
                .setMaxParallelism(params.getInt("mp4", 8))
                .slotSharingGroup("g4");

        // Centralized to predict travel time
        DataStream<Tuple18<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> afterLastAverageSpeed = afterAverageSpeed
                .keyBy(LinearRoadSource.Query_ID)
                .flatMap(new LastAverageSpeed(params.getInt("op5Delay", 1000)))
                .disableChaining()
                .name("Last Average Speed")
                .uid("op5")
                .setParallelism(params.getInt("p5", 1))
                .setMaxParallelism(params.getInt("mp5", 8))
                .slotSharingGroup("g5");

        DataStream<Tuple18<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> afterCountVehicles = source
                .keyBy(LinearRoadSource.Seg)
                .flatMap(new CountVehicles(params.getInt("op6Delay", 1000)))
                .disableChaining()
                .name("Count Vehicles")
                .uid("op6")
                .setParallelism(params.getInt("p6", 1))
                .setMaxParallelism(params.getInt("mp6", 8))
                .slotSharingGroup("g6");

        DataStream<Tuple18<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> afterTollNotification = source.union(afterAccidentDetection).union(afterLastAverageSpeed).union(afterCountVehicles)
                .keyBy(LinearRoadSource.Seg)
                .flatMap(new TollNotification(params.getInt("op7Delay", 1000)))
                .disableChaining()
                .name("Toll Notification")
                .uid("op7")
                .setParallelism(params.getInt("p7", 1))
                .setMaxParallelism(params.getInt("mp7", 8))
                .slotSharingGroup("g7");

        DataStream<Tuple18<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> afterAccountBalance = source.union(afterTollNotification)
                .keyBy(LinearRoadSource.Car_ID)
                .flatMap(new AccountBalance(params.getInt("op8Delay", 1000)))
                .disableChaining()
                .name("Account Balance")
                .uid("op8")
                .setParallelism(params.getInt("p8", 1))
                .setMaxParallelism(params.getInt("mp8", 8))
                .slotSharingGroup("g8");

        source.union(afterAccountBalance)
                .keyBy(LinearRoadSource.Car_ID)
                .flatMap(new DailyExpense(params.getInt("op9Delay", 1000)))
                .disableChaining()
                .name("Daily Expense")
                .uid("op9")
                .setParallelism(params.getInt("p9", 1))
                .setMaxParallelism(params.getInt("mp9", 8))
                .slotSharingGroup("g9");

        env.execute();
    }

    private static class LinearRoadSource extends RichParallelSourceFunction<Tuple18<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> {
        private volatile boolean running = true;
        private static final int Type = 0;
        private static final int Car_ID = 1;
        private static final int Speed = 2;
        private static final int Xway = 3;
        private static final int Lane = 4;
        private static final int Dir = 5;
        private static final int Seg = 6;
        private static final int Pos = 7;
        private static final int Time = 8;
        private static final int Query_ID = 9;
        private static final int Q_Start = 10;
        private static final int Q_End = 11;
        private static final int Q_DayOfWeek = 12;
        private static final int Q_Minutes = 13;
        private static final int Q_Day = 14;
        private final String FILE;
        private final long warmup, warmp_rate, skipCount;

        public LinearRoadSource(String FILE, long warmup, long warmup_rate, long skipCount) {
            this.FILE = FILE;
            this.warmup = warmup;
            this.warmp_rate = warmup_rate;
            this.skipCount = skipCount;
        }

        @Override
        public void run(SourceContext<Tuple18<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> ctx) throws Exception {
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
                    int key = count % 1000000;
                    ctx.collect(Tuple18.of(key, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, System.currentTimeMillis(), (long) count));
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
                    if (sCurrentLine.equals("END")) {
                        sleepCnt++;
                        if (counter == 0) {
                            noRecSleepCnt++;
                            System.out.println("no record in this sleep !" + noRecSleepCnt);
                        }
                        // System.out.println("output rate: " + counter);
                        if (sleepCnt <= skipCount) {
                            for (int i = 0; i < warmp_rate / 20; i++) {
                                int key = count % 1000000;
                                ctx.collect(Tuple18.of(key, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, System.currentTimeMillis(), (long) count));
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

                    if (sCurrentLine.split(",").length < 10) {
                        continue;
                    }

                    if (sleepCnt > skipCount) {
                        Long ts = System.currentTimeMillis();
                        String msg = sCurrentLine;
                        List<String> stockArr = Arrays.asList(msg.split(","));
                        ctx.collect(new Tuple18<>(
                                Integer.parseInt(stockArr.get(0)),
                                Integer.parseInt(stockArr.get(1)),
                                Integer.parseInt(stockArr.get(2)),
                                Integer.parseInt(stockArr.get(3)),
                                Integer.parseInt(stockArr.get(4)),
                                Integer.parseInt(stockArr.get(5)),
                                Integer.parseInt(stockArr.get(6)),
                                Integer.parseInt(stockArr.get(7)),
                                Integer.parseInt(stockArr.get(8)),
                                Integer.parseInt(stockArr.get(9)),
                                Integer.parseInt(stockArr.get(10)),
                                Integer.parseInt(stockArr.get(11)),
                                Integer.parseInt(stockArr.get(12)),
                                Integer.parseInt(stockArr.get(13)),
                                Integer.parseInt(stockArr.get(14)),
                                0, ts, (long) count));
                        count++;
                    }
                    counter++;
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (stream != null) stream.close();
                    if (br != null) br.close();
                } catch (IOException ex) {
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

    public static final class AccidentDetection extends RichFlatMapFunction<
            Tuple18<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>,
            Tuple18<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> {

        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private transient MapState<Integer, Integer> carLastPos, carStayLength;
        private int averageDelay; // Microsecond

        public AccidentDetection(int _averageDelay) {
            this.averageDelay = _averageDelay;
        }

        @Override
        public void flatMap(Tuple18<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long> input, Collector<Tuple18<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> out) throws Exception {
            int car_id = input.f1, pos = input.f7;
            if (carLastPos.contains(car_id) && carLastPos.get(car_id) == pos) {
                int stayLength = carStayLength.get(car_id) + 1;
                if (stayLength >= 4) {
                    out.collect(new Tuple18<>(
                            input.f0,
                            car_id,
                            input.f2,
                            input.f3,
                            input.f4,
                            input.f5,
                            input.f6,
                            input.f7,
                            input.f8,
                            input.f9,
                            input.f10,
                            input.f11,
                            input.f12,
                            input.f13,
                            input.f14,
                            AccidentDetection_Output,
                            input.f16,
                            input.f17));
                }
                carStayLength.put(car_id, stayLength);
            } else {
                carLastPos.put(car_id, pos);
                carStayLength.put(car_id, 1);
            }
            delay(averageDelay);
        }

        private void delay(long interval) {
            Double ranN = randomGen.nextGaussian(interval, 1);
            ranN = ranN * 1000;
            long delay = ranN.intValue();
            if (delay < 0) delay = interval * 1000;
            Long start = System.nanoTime();
            while (System.nanoTime() - start < delay) {
            }
        }

        @Override
        public void open(Configuration config) {
            MapStateDescriptor<Integer, Integer> descriptor =
                    new MapStateDescriptor<>("accident-detection-pos", Integer.class, Integer.class);
            carLastPos = getRuntimeContext().getMapState(descriptor);
            descriptor = new MapStateDescriptor<>("accident-detection-stay", Integer.class, Integer.class);
            carStayLength = getRuntimeContext().getMapState(descriptor);
        }
    }

    public static final class AccidentNotification extends RichMapFunction<
            Tuple18<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>,
            Tuple2<Integer, Integer>> {
        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private int averageDelay; // Microsecond

        public AccidentNotification(int _averageDelay) {
            this.averageDelay = _averageDelay;
        }

        @Override
        public Tuple2<Integer, Integer> map(Tuple18<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long> input) throws Exception {
            int notification_l = 0, notification_r = 0;
            if (input.f15 == Source_Output) {
                // TODO: record cars in each seg
            } else if (input.f15 == AccidentDetection_Output) {
                int dir = input.f5, seg = input.f6, pos = input.f7;
                if (dir == 0) {
                    notification_l = seg - 4;
                    if (notification_l < 0) {
                        notification_l = 0;
                    }
                    notification_r = seg;
                } else {
                    notification_r = seg + 4;
                    if (notification_r >= 100) {
                        notification_r = 99;
                    }
                    notification_l = seg;
                }
                System.out.println("Accident Notification for Seg: " + notification_l + "-" + notification_r);
                // TODO: info the cars in the segs
            }
            delay(averageDelay);
            long currentTime = System.currentTimeMillis();
            System.out.println("GT: " + input.f0 + ", " + currentTime + ", " + (currentTime - input.f3) + ", " + input.f4);
            return new Tuple2<>(notification_l, notification_r);
        }

        private void delay(long interval) {
            Double ranN = randomGen.nextGaussian(interval, 1);
            ranN = ranN * 1000;
            long delay = ranN.intValue();
            if (delay < 0) delay = interval * 1000;
            Long start = System.nanoTime();
            while (System.nanoTime() - start < delay) {
            }
        }
    }

    public static final class AverageSpeed extends RichFlatMapFunction<
            Tuple18<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>,
            Tuple18<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> {

        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private transient MapState<Integer, Integer> totalSpeedPerSeg, totalCarsPerSeg, carSpeed, carSeg;
        private int averageDelay; // Microsecond

        public AverageSpeed(int _averageDelay) {
            this.averageDelay = _averageDelay;
        }

        @Override
        public void flatMap(Tuple18<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long> input, Collector<Tuple18<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> out) throws Exception {
            int car_id = input.f1, seg = input.f6, speed = input.f2;
            if (carSeg.contains(car_id)) {
                int old_seg = carSeg.get(car_id);
                int old_speed = carSpeed.get(car_id);
                int old_cars = totalCarsPerSeg.get(old_seg);
                totalCarsPerSeg.put(old_seg, old_cars - 1);
                int old_totalspeed = totalSpeedPerSeg.get(old_seg);
                totalSpeedPerSeg.put(old_seg, old_totalspeed - old_speed);
                int old_seg_avgSpeed = 0;
                if (old_cars > 1) {
                    old_seg_avgSpeed = (old_totalspeed - old_speed) / (old_cars - 1);
                }
                out.collect(new Tuple18<>(
                        input.f0,
                        car_id,
                        old_seg_avgSpeed,
                        input.f3,
                        input.f4,
                        input.f5,
                        old_seg,
                        input.f7,
                        input.f8,
                        input.f9,
                        input.f10,
                        input.f11,
                        input.f12,
                        input.f13,
                        input.f14,
                        AverageSpeed_Output,
                        input.f16,
                        input.f17));
            }
            carSeg.put(car_id, seg);
            carSpeed.put(car_id, speed);
            if (!totalSpeedPerSeg.contains(seg)) {
                totalSpeedPerSeg.put(seg, speed);
                totalCarsPerSeg.put(seg, 1);
            } else {
                int old_cars = totalCarsPerSeg.get(seg);
                int old_totalspeed = totalSpeedPerSeg.get(seg);
                totalSpeedPerSeg.put(seg, old_totalspeed + speed);
                totalCarsPerSeg.put(seg, old_cars + 1);
            }
            int avg_speed = totalSpeedPerSeg.get(seg) / totalCarsPerSeg.get(seg);
            out.collect(new Tuple18<>(
                    input.f0,
                    car_id,
                    avg_speed,
                    input.f3,
                    input.f4,
                    input.f5,
                    seg,
                    input.f7,
                    input.f8,
                    input.f9,
                    input.f10,
                    input.f11,
                    input.f12,
                    input.f13,
                    input.f14,
                    AverageSpeed_Output,
                    input.f16,
                    input.f17));
            delay(averageDelay);
        }

        private void delay(long interval) {
            Double ranN = randomGen.nextGaussian(interval, 1);
            ranN = ranN * 1000;
            long delay = ranN.intValue();
            if (delay < 0) delay = interval * 1000;
            Long start = System.nanoTime();
            while (System.nanoTime() - start < delay) {
            }
        }

        @Override
        public void open(Configuration config) {
            MapStateDescriptor<Integer, Integer> descriptor =
                    new MapStateDescriptor<>("average-speed-carseg", Integer.class, Integer.class);
            carSeg = getRuntimeContext().getMapState(descriptor);
            descriptor = new MapStateDescriptor<>("average-speed-carspeed", Integer.class, Integer.class);
            carSpeed = getRuntimeContext().getMapState(descriptor);
            descriptor = new MapStateDescriptor<>("average-speed-segcars", Integer.class, Integer.class);
            totalCarsPerSeg = getRuntimeContext().getMapState(descriptor);
            descriptor = new MapStateDescriptor<>("average-speed-segspeed", Integer.class, Integer.class);
            totalSpeedPerSeg = getRuntimeContext().getMapState(descriptor);
        }
    }

    public static final class LastAverageSpeed extends RichFlatMapFunction<
            Tuple18<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>,
            Tuple18<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> {

        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private transient MapState<Integer, Integer> speedPerSeg;
        private int averageDelay; // Microsecond

        public LastAverageSpeed(int _averageDelay) {
            this.averageDelay = _averageDelay;
        }

        @Override
        public void flatMap(Tuple18<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long> input, Collector<Tuple18<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> out) throws Exception {
            if (input.f15 == AverageSpeed_Output){
                int seg = input.f6, average_speed = input.f2;
                speedPerSeg.put(seg, average_speed);
            }

            if (input.f0 == 4){
                int start_seg = input.f10, end_seg = input.f11;
                if(start_seg > end_seg){
                    int t = start_seg;
                    start_seg = end_seg;
                    end_seg = t;
                }
                int totalTime = 0;
                for(int i = start_seg; i <= end_seg; i++){
                    if(speedPerSeg.contains(i)) {
                        int speed = speedPerSeg.get(i);
                        int time;
                        if(speed == 0){
                            time = 86400;
                        }else {
                            time = 3600 / speedPerSeg.get(i);
                        }
                        totalTime += time;
                    }
                }
                System.out.println("Travel Time Estimation from " + input.f10 + " to "  + input.f11 + " is " + totalTime + ".");
            }
            out.collect(new Tuple18<>(
                    input.f0,
                    input.f1,
                    input.f2,
                    input.f3,
                    input.f4,
                    input.f5,
                    input.f6,
                    input.f7,
                    input.f8,
                    input.f9,
                    input.f10,
                    input.f11,
                    input.f12,
                    input.f13,
                    input.f14,
                    LastAverageSpeed_Output,
                    input.f16,
                    input.f17
            ));
            delay(averageDelay);
        }

        private void delay(long interval) {
            Double ranN = randomGen.nextGaussian(interval, 1);
            ranN = ranN * 1000;
            long delay = ranN.intValue();
            if (delay < 0) delay = interval * 1000;
            Long start = System.nanoTime();
            while (System.nanoTime() - start < delay) {
            }
        }

        @Override
        public void open(Configuration config) {
            MapStateDescriptor<Integer, Integer> descriptor =
                    new MapStateDescriptor<>("last-average-speed-per-seg", Integer.class, Integer.class);
            speedPerSeg = getRuntimeContext().getMapState(descriptor);
        }
    }

    public static final class CountVehicles extends RichFlatMapFunction<
            Tuple18<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>,
            Tuple18<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> {

        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private transient MapState<Integer, Integer> totalCarsPerSeg, carSeg;
        private int averageDelay; // Microsecond

        public CountVehicles(int _averageDelay) {
            this.averageDelay = _averageDelay;
        }

        @Override
        public void flatMap(Tuple18<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long> input, Collector<Tuple18<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> out) throws Exception {
            int car_id = input.f1, seg = input.f6;
            if (carSeg.contains(car_id)) {
                int old_seg = carSeg.get(car_id);
                int old_cars = totalCarsPerSeg.get(old_seg);
                totalCarsPerSeg.put(old_seg, old_cars - 1);
                out.collect(new Tuple18<>(
                        input.f0,
                        car_id,
                        old_cars - 1,
                        input.f3,
                        input.f4,
                        input.f5,
                        old_seg,
                        input.f7,
                        input.f8,
                        input.f9,
                        input.f10,
                        input.f11,
                        input.f12,
                        input.f13,
                        input.f14,
                        CountVehicles_Output,
                        input.f16,
                        input.f17));
            }
            carSeg.put(car_id, seg);
            int old_cars;
            if (!totalCarsPerSeg.contains(seg)) {
                old_cars = 0;
            } else {
                old_cars = totalCarsPerSeg.get(seg);
            }
            totalCarsPerSeg.put(seg, old_cars + 1);
            out.collect(new Tuple18<>(
                    input.f0,
                    car_id,
                    old_cars + 1,
                    input.f3,
                    input.f4,
                    input.f5,
                    seg,
                    input.f7,
                    input.f8,
                    input.f9,
                    input.f10,
                    input.f11,
                    input.f12,
                    input.f13,
                    input.f14,
                    CountVehicles_Output,
                    input.f16,
                    input.f17));
            delay(averageDelay);
        }

        private void delay(long interval) {
            Double ranN = randomGen.nextGaussian(interval, 1);
            ranN = ranN * 1000;
            long delay = ranN.intValue();
            if (delay < 0) delay = interval * 1000;
            Long start = System.nanoTime();
            while (System.nanoTime() - start < delay) {
            }
        }

        @Override
        public void open(Configuration config) {
            MapStateDescriptor<Integer, Integer> descriptor =
                    new MapStateDescriptor<>("count-vehicle-carseg", Integer.class, Integer.class);
            carSeg = getRuntimeContext().getMapState(descriptor);
            descriptor = new MapStateDescriptor<>("count-vehicle-carspeed", Integer.class, Integer.class);
            totalCarsPerSeg = getRuntimeContext().getMapState(descriptor);
        }
    }

    public static final class TollNotification extends RichFlatMapFunction<
            Tuple18<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>,
            Tuple18<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> {

        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private transient MapState<Integer, Integer> segAverageSpeed, segLastAccident, segCarCounts;
        private int averageDelay; // Microsecond

        public TollNotification(int _averageDelay) {
            this.averageDelay = _averageDelay;
        }

        @Override
        public void flatMap(Tuple18<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long> input, Collector<Tuple18<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> out) throws Exception {
            int car_id = input.f1, seg = input.f6, source = input.f15, time = input.f8;
            if (source == AccidentDetection_Output){
                segLastAccident.put(seg, time);
                long currentTime = System.currentTimeMillis();
                System.out.println("GT: " + input.f0 + ", " + currentTime + ", " + (currentTime - input.f3) + ", " + input.f4);
            }else if(source == LastAverageSpeed_Output){
                segAverageSpeed.put(seg, input.f2);
                long currentTime = System.currentTimeMillis();
                System.out.println("GT: " + input.f0 + ", " + currentTime + ", " + (currentTime - input.f3) + ", " + input.f4);
            }else if(source == CountVehicles_Output){
                segCarCounts.put(seg, input.f2);
                long currentTime = System.currentTimeMillis();
                System.out.println("GT: " + input.f0 + ", " + currentTime + ", " + (currentTime - input.f3) + ", " + input.f4);
            }else if(source == Source_Output){
                int price = 0, carCounts = 0, averageSpeed = 50;
                if (segCarCounts.contains(seg)){
                    carCounts = segCarCounts.get(seg);
                }
                if (segAverageSpeed.contains(seg)){
                    averageSpeed = segAverageSpeed.get(seg);
                }
                price = carCounts * 5 + (100 - averageSpeed);
                if (segLastAccident.contains(seg) && segLastAccident.get(seg) >= time - 300){
                    price /= 2;
                }
                System.out.println("Toll Notification: car " + car_id + " enter seg " + seg + " price " + price);
                out.collect(new Tuple18<>(
                        input.f0,
                        car_id,
                        price,
                        input.f3,
                        input.f4,
                        input.f5,
                        input.f6,
                        input.f7,
                        input.f8,
                        input.f9,
                        input.f10,
                        input.f11,
                        input.f12,
                        input.f13,
                        input.f14,
                        TollNotification_Output,
                        input.f16,
                        input.f17));
            }
            delay(averageDelay);
        }

        private void delay(long interval) {
            Double ranN = randomGen.nextGaussian(interval, 1);
            ranN = ranN * 1000;
            long delay = ranN.intValue();
            if (delay < 0) delay = interval * 1000;
            Long start = System.nanoTime();
            while (System.nanoTime() - start < delay) {
            }
        }

        @Override
        public void open(Configuration config) {
            MapStateDescriptor<Integer, Integer> descriptor =
                    new MapStateDescriptor<>("toll-notification-segspeed", Integer.class, Integer.class);
            segAverageSpeed = getRuntimeContext().getMapState(descriptor);
            descriptor = new MapStateDescriptor<>("toll-notification-segaccident", Integer.class, Integer.class);
            segLastAccident = getRuntimeContext().getMapState(descriptor);
            descriptor = new MapStateDescriptor<>("toll-notification-segcars", Integer.class, Integer.class);
            segCarCounts = getRuntimeContext().getMapState(descriptor);
        }
    }

    public static final class AccountBalance extends RichFlatMapFunction<
            Tuple18<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>,
            Tuple18<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> {

        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private transient MapState<Integer, Integer> balancePerCar;
        private int averageDelay; // Microsecond

        public AccountBalance(int _averageDelay) {
            this.averageDelay = _averageDelay;
        }

        @Override
        public void flatMap(Tuple18<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long> input, Collector<Tuple18<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> out) throws Exception {
            int car_id = input.f1, source = input.f15;
            if (source == TollNotification_Output){
                int old_balance = 0;
                if(balancePerCar.contains(car_id)){
                    old_balance = balancePerCar.get(car_id);
                }
                balancePerCar.put(car_id, old_balance + input.f2);
                out.collect(new Tuple18<>(
                        input.f0,
                        input.f1,
                        input.f2,
                        input.f3,
                        input.f4,
                        input.f5,
                        input.f6,
                        input.f7,
                        input.f8,
                        input.f9,
                        input.f10,
                        input.f11,
                        input.f12,
                        input.f13,
                        input.f14,
                        AccountBalance_Output,
                        input.f16,
                        input.f17));
            }else if(source == Source_Output){
                if (input.f0 == 2) {
                    int balance = 0;
                    if (balancePerCar.contains(car_id)){
                        balance = balancePerCar.get(car_id);
                    }
                    System.out.println("Account Balance: car " + car_id + " balance " + balance);
                    long currentTime = System.currentTimeMillis();
                    System.out.println("GT: " + input.f0 + ", " + currentTime + ", " + (currentTime - input.f3) + ", " + input.f4);
                }
            }
            delay(averageDelay);
        }

        private void delay(long interval) {
            Double ranN = randomGen.nextGaussian(interval, 1);
            ranN = ranN * 1000;
            long delay = ranN.intValue();
            if (delay < 0) delay = interval * 1000;
            Long start = System.nanoTime();
            while (System.nanoTime() - start < delay) {
            }
        }

        @Override
        public void open(Configuration config) {
            MapStateDescriptor<Integer, Integer> descriptor =
                    new MapStateDescriptor<>("account-balance-carbalance", Integer.class, Integer.class);
            balancePerCar = getRuntimeContext().getMapState(descriptor);
        }
    }

    public static final class DailyExpense extends RichFlatMapFunction<
            Tuple18<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>,
            Tuple18<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> {

        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private transient MapState<Integer, Integer> dailyExpensePerCar, dayPerCar;
        private int averageDelay; // Microsecond

        public DailyExpense(int _averageDelay) {
            this.averageDelay = _averageDelay;
        }

        @Override
        public void flatMap(Tuple18<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long> input, Collector<Tuple18<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> out) throws Exception {
            int car_id = input.f1, source = input.f15, time = input.f8;
            if (source == AccidentDetection_Output){
                if(dayPerCar.contains(car_id) && time - dayPerCar.get(car_id) >= 86400){
                    dayPerCar.put(car_id, time);
                    dailyExpensePerCar.remove(car_id);
                }
                int old_expense = 0;
                if(dailyExpensePerCar.contains(car_id)){
                    old_expense = dailyExpensePerCar.get(car_id);
                }
                dailyExpensePerCar.put(car_id, old_expense + input.f2);
                long currentTime = System.currentTimeMillis();
                System.out.println("GT: " + input.f0 + ", " + currentTime + ", " + (currentTime - input.f3) + ", " + input.f4);
            }else if(source == Source_Output){
                if (input.f0 == 3) {
                    int expense = 0;
                    if (dailyExpensePerCar.contains(car_id)){
                        expense = dailyExpensePerCar.get(car_id);
                    }
                    System.out.println("Daily Expense: car " + car_id + " expense " + expense);
                    long currentTime = System.currentTimeMillis();
                    System.out.println("GT: " + input.f0 + ", " + currentTime + ", " + (currentTime - input.f3) + ", " + input.f4);
                }
            }
            delay(averageDelay);
        }

        private void delay(long interval) {
            Double ranN = randomGen.nextGaussian(interval, 1);
            ranN = ranN * 1000;
            long delay = ranN.intValue();
            if (delay < 0) delay = interval * 1000;
            Long start = System.nanoTime();
            while (System.nanoTime() - start < delay) {
            }
        }

        @Override
        public void open(Configuration config) {
            MapStateDescriptor<Integer, Integer> descriptor =
                    new MapStateDescriptor<>("daily-expense-carexpense", Integer.class, Integer.class);
            dailyExpensePerCar = getRuntimeContext().getMapState(descriptor);
            descriptor = new MapStateDescriptor<>("daily-expense-carday", Integer.class, Integer.class);
            dayPerCar = getRuntimeContext().getMapState(descriptor);
        }
    }

}
