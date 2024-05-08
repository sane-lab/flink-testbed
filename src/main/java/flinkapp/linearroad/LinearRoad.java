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
        DataStreamSource<Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> source =
                env.addSource(new LinearRoadSource(params.get("file_name", "/home/samza/LR_data/3hr.txt"),
                                params.getLong("warmup_time", 30L) * 1000,
                                params.getLong("warmup_rate", 1500L),
                                params.getLong("skip_interval", 0L) * 20))
                        .setParallelism(params.getInt("p1", 1));

        DataStream<Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> afterDispatcher = source
                .keyBy(LinearRoadSource.Car_ID)
                .flatMap(new Dispatcher(10))
                .disableChaining()
                .name("Dispatcher")
                .uid("op1")
                .setParallelism(params.getInt("p1", 1))
                .setMaxParallelism(params.getInt("mp1", 64))
                .slotSharingGroup("g1");

        DataStream<Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> afterAccidentDetection = afterDispatcher
                .keyBy(LinearRoadSource.Car_ID)
                .flatMap(new AccidentDetection(params.getInt("op2Delay", 1000)))
                .disableChaining()
                .name("Accident Detection")
                .uid("op2")
                .setParallelism(params.getInt("p2", 1))
                .setMaxParallelism(params.getInt("mp2", 8))
                .slotSharingGroup("g2");
//        afterAccidentDetection//.union(afterDispatcher)
//                .keyBy(LinearRoadSource.Car_ID) //.keyBy(LinearRoadSource.Seg_ID)
//                .map(new AccidentNotification(params.getInt("op3Delay", 1000)))
//                .disableChaining()
//                .name("Accident Notification")
//                .uid("op3")
//                .setParallelism(params.getInt("p3", 1))
//                .setMaxParallelism(params.getInt("mp3", 8))
//                .slotSharingGroup("g3");

//        DataStream<Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> afterAverageSpeed = afterDispatcher
//                .keyBy(LinearRoadSource.Car_ID) // .keyBy(LinearRoadSource.Seg_ID)
//                .flatMap(new AverageSpeed(params.getInt("op4Delay", 1000)))
//                .disableChaining()
//                .name("Average Speed")
//                .uid("op4")
//                .setParallelism(params.getInt("p4", 1))
//                .setMaxParallelism(params.getInt("mp4", 8))
//                .slotSharingGroup("g4");
//
//        // Centralized to predict travel time
//        DataStream<Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> afterLastAverageSpeed = afterAverageSpeed
//                .keyBy(LinearRoadSource.Car_ID) // .keyBy(LinearRoadSource.Query_ID)
//                .flatMap(new LastAverageSpeed(params.getInt("op5Delay", 1000)))
//                .disableChaining()
//                .name("Last Average Speed")
//                .uid("op5")
//                .setParallelism(params.getInt("p5", 1))
//                .setMaxParallelism(params.getInt("mp5", 8))
//                .slotSharingGroup("g5");
        DataStream<Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> afterAverageSpeed = afterDispatcher
                .keyBy(LinearRoadSource.Car_ID) // .keyBy(LinearRoadSource.Seg_ID)
                .flatMap(new AverageSpeedAndLastAverageSpeed(params.getInt("op3Delay", 1000)))
                .disableChaining()
                .name("Average Speed and Last Average Speed")
                .uid("op3")
                .setParallelism(params.getInt("p3", 1))
                .setMaxParallelism(params.getInt("mp3", 8))
                .slotSharingGroup("g3");


        DataStream<Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> afterCountVehicles = afterDispatcher
                .keyBy(LinearRoadSource.Car_ID) // .keyBy(LinearRoadSource.Seg_ID)
                .flatMap(new CountVehicles(params.getInt("op4Delay", 1000)))
                .disableChaining()
                .name("Count Vehicles")
                .uid("op4")
                .setParallelism(params.getInt("p4", 1))
                .setMaxParallelism(params.getInt("mp4", 8))
                .slotSharingGroup("g4");

//        DataStream<Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> afterTollNotification = afterAccidentDetection.union(afterLastAverageSpeed).union(afterCountVehicles) // .union(afterDispatcher)
//                .keyBy(LinearRoadSource.Car_ID) // .keyBy(LinearRoadSource.Seg_ID)
//                .flatMap(new TollNotification(params.getInt("op7Delay", 1000)))
//                .disableChaining()
//                .name("Toll Notification")
//                .uid("op7")
//                .setParallelism(params.getInt("p7", 1))
//                .setMaxParallelism(params.getInt("mp7", 8))
//                .slotSharingGroup("g7");
//
//        DataStream<Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> afterAccountBalance = afterTollNotification // .union(afterDispatcher)
//                .keyBy(LinearRoadSource.Car_ID)
//                .flatMap(new AccountBalanceAndDailyExpense(params.getInt("op8Delay", 1000)))
//                .disableChaining()
//                .name("Account Balance")
//                .uid("op8")
//                .setParallelism(params.getInt("p8", 1))
//                .setMaxParallelism(params.getInt("mp8", 8))
//                .slotSharingGroup("g8");

        DataStream<Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> afterTollNotification = afterAccidentDetection.union(afterAverageSpeed).union(afterCountVehicles) // .union(afterDispatcher)
                .keyBy(LinearRoadSource.Car_ID) // .keyBy(LinearRoadSource.Seg_ID)
                .flatMap(new TollNotificationAndAccountBalanceAndDailyExpense(params.getInt("op5Delay", 1000)))
                .disableChaining()
                .name("Toll Notification and Account Balance")
                .uid("op5")
                .setParallelism(params.getInt("p5", 1))
                .setMaxParallelism(params.getInt("mp5", 8))
                .slotSharingGroup("g5");


//        afterAccountBalance
//                .keyBy(LinearRoadSource.Car_ID)
//                .flatMap(new DailyExpense(params.getInt("op9Delay", 1000)))
//                .disableChaining()
//                .name("Daily Expense")
//                .uid("op9")
//                .setParallelism(params.getInt("p9", 1))
//                .setMaxParallelism(params.getInt("mp9", 8))
//                .slotSharingGroup("g9");

        env.execute();
    }

    public static class LinearRoadSource extends RichParallelSourceFunction<Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> {
        private volatile boolean running = true;
        private static final int Seg_ID = 0;
        private static final int Type = 1;
        private static final int Car_ID = 2;
        private static final int Speed = 3;
        private static final int Xway = 4;
        private static final int Lane = 5;
        private static final int Dir = 6;
        private static final int Seg = 7;
        private static final int Pos = 8;
        private static final int Time = 9;
        private static final int Query_ID = 10;
        private static final int Q_Start = 11;
        private static final int Q_End = 12;
        private static final int Q_DayOfWeek = 13;
        private static final int Q_Minutes = 14;
        private static final int Q_Day = 15;
        private static final int Output_Operator = 16;
        private static final int Arrival_Time = 17;
        private static final int Tuple_Number = 18;

        private final String FILE;
        private final long warmup, warmp_rate, skipCount;

        public static String getSegID(int seg){
            return "A" + seg;
        }

        public static String getCarID(int car_ID){
            return String.format("A%6d",car_ID);
        }


        public LinearRoadSource(String FILE, long warmup, long warmup_rate, long skipCount) {
            this.FILE = FILE;
            this.warmup = warmup;
            this.warmp_rate = warmup_rate;
            this.skipCount = skipCount;
        }

        @Override
        public void run(SourceContext<Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> ctx) throws Exception {
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
                    int car_id = count % 1000000;
                    int seg = count % 100;
                    String seg_ID = getSegID(seg);
                    ctx.collect(Tuple19.of(seg_ID, 0, getCarID(car_id), 0, 0, 0, 0, seg, 0, 0, 0, 0, 0, 0, 0, 0, 0, System.currentTimeMillis(), (long) count));
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
                                int car_id = count % 1000000;
                                int seg = count % 100;
                                String seg_ID = getSegID(seg);
                                ctx.collect(Tuple19.of(seg_ID, 0, getCarID(car_id), 0, 0, 0, 0, seg, 0, 0, 0, 0, 0, 0, 0, 0, 0, System.currentTimeMillis(), (long) count));
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
                        int seg = Integer.parseInt(stockArr.get(Seg - 1)), car_id = Integer.parseInt(stockArr.get(Car_ID - 1));
                        ctx.collect(new Tuple19<>(
                                getSegID(seg),
                                Integer.parseInt(stockArr.get(0)),
                                getCarID(car_id),
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

    public static final class Dispatcher extends RichFlatMapFunction<
            Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>,
            Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> {

        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private int averageDelay; // Microsecond

        public Dispatcher(int _averageDelay) {
            this.averageDelay = _averageDelay;
        }

        @Override
        public void flatMap(Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long> input, Collector<Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> out) throws Exception {
            out.collect(new Tuple19<>(
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
                    input.f15,
                    Source_Output,
                    input.f17,
                    input.f18));
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
    }

    public static final class AccidentDetection extends RichFlatMapFunction<
            Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>,
            Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> {

        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private transient MapState<String, Integer> carLastPos, carStayLength;
        private int averageDelay; // Microsecond

        public AccidentDetection(int _averageDelay) {
            this.averageDelay = _averageDelay;
        }

        @Override
        public void flatMap(Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long> input, Collector<Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> out) throws Exception {
            String car_id = input.f2;
            int pos = input.f8;
            if (carLastPos.contains(car_id) && carLastPos.get(car_id) == pos) {
                int stayLength = carStayLength.get(car_id) + 1;
                if (stayLength >= 4) {
                    out.collect(new Tuple19<>(
                            input.f0,
                            input.f1,
                            car_id,
                            1,
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
                            input.f15,
                            AccidentDetection_Output,
                            input.f17,
                            input.f18));
                }else{
                    out.collect(new Tuple19<>(
                            input.f0,
                            input.f1,
                            car_id,
                            0,
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
                            input.f15,
                            AccidentDetection_Output,
                            input.f17,
                            input.f18));
                }
                carStayLength.put(car_id, stayLength);
            } else {
                out.collect(new Tuple19<>(
                        input.f0,
                        input.f1,
                        car_id,
                        0,
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
                        input.f15,
                        AccidentDetection_Output,
                        input.f17,
                        input.f18));
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
            MapStateDescriptor<String, Integer> descriptor =
                    new MapStateDescriptor<>("accident-detection-pos", String.class, Integer.class);
            carLastPos = getRuntimeContext().getMapState(descriptor);
            descriptor = new MapStateDescriptor<>("accident-detection-stay", String.class, Integer.class);
            carStayLength = getRuntimeContext().getMapState(descriptor);
        }
    }

    public static final class AccidentNotification extends RichMapFunction<
            Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>,
            Tuple2<Integer, Integer>> {
        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private int averageDelay; // Microsecond

        public AccidentNotification(int _averageDelay) {
            this.averageDelay = _averageDelay;
        }

        @Override
        public Tuple2<Integer, Integer> map(Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long> input) throws Exception {
            int notification_l = 0, notification_r = 0;
            if (input.f16 == Source_Output) {
                // TODO: record cars in each seg
            } else if (input.f16 == AccidentDetection_Output) {
                int dir = input.f6, seg = input.f7, pos = input.f8;
                int accident_flag = input.f3;
                if(accident_flag == 1) {
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
            }
            delay(averageDelay);
            long currentTime = System.currentTimeMillis();
            System.out.println("GT: " + input.f0 + "-" + input.f2 + ", " + currentTime + ", " + (currentTime - input.f17) + ", " + input.f18);
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
            Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>,
            Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> {

        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private transient MapState<Integer, Integer> totalSpeedPerSeg, totalCarsPerSeg;
        private transient MapState<String, Integer> carSpeed, carSeg;
        private int averageDelay; // Microsecond

        public AverageSpeed(int _averageDelay) {
            this.averageDelay = _averageDelay;
        }

        @Override
        public void flatMap(Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long> input, Collector<Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> out) throws Exception {
            String car_id = input.f2;
            int seg = input.f7, speed = input.f3;
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
                out.collect(new Tuple19<>(
                        LinearRoadSource.getSegID(old_seg),
                        input.f1,
                        car_id,
                        old_seg_avgSpeed,
                        input.f4,
                        input.f5,
                        input.f6,
                        old_seg,
                        input.f8,
                        input.f9,
                        input.f10,
                        input.f11,
                        input.f12,
                        input.f13,
                        input.f14,
                        input.f15,
                        AverageSpeed_Output,
                        input.f17,
                        input.f18));
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
            out.collect(new Tuple19<>(
                    LinearRoadSource.getSegID(seg),
                    input.f1,
                    car_id,
                    avg_speed,
                    input.f4,
                    input.f5,
                    input.f6,
                    seg,
                    input.f8,
                    input.f9,
                    input.f10,
                    input.f11,
                    input.f12,
                    input.f13,
                    input.f14,
                    input.f15,
                    AverageSpeed_Output,
                    input.f17,
                    input.f18));
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
            MapStateDescriptor<String, Integer> descriptor =
                    new MapStateDescriptor<>("average-speed-carseg", String.class, Integer.class);
            carSeg = getRuntimeContext().getMapState(descriptor);
            descriptor = new MapStateDescriptor<>("average-speed-carspeed", String.class, Integer.class);
            carSpeed = getRuntimeContext().getMapState(descriptor);
            MapStateDescriptor<Integer, Integer> descriptor1 = new MapStateDescriptor<>("average-speed-segcars", Integer.class, Integer.class);
            totalCarsPerSeg = getRuntimeContext().getMapState(descriptor1);
            descriptor1 = new MapStateDescriptor<>("average-speed-segspeed", Integer.class, Integer.class);
            totalSpeedPerSeg = getRuntimeContext().getMapState(descriptor1);
        }
    }

    public static final class LastAverageSpeed extends RichFlatMapFunction<
            Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>,
            Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> {

        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private transient MapState<Integer, Integer> speedPerSeg;
        private int averageDelay; // Microsecond

        public LastAverageSpeed(int _averageDelay) {
            this.averageDelay = _averageDelay;
        }

        @Override
        public void flatMap(Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long> input, Collector<Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> out) throws Exception {
            if (input.f16 == AverageSpeed_Output){
                int seg = input.f7, average_speed = input.f3;
                speedPerSeg.put(seg, average_speed);
            }

            if (input.f1 == 4){
                int start_seg = input.f11, end_seg = input.f12;
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
                System.out.println("Travel Time Estimation from " + start_seg + " to "  + end_seg + " is " + totalTime + ".");
            }
            out.collect(new Tuple19<>(
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
                    input.f15,
                    LastAverageSpeed_Output,
                    input.f17,
                    input.f18
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

    public static final class AverageSpeedAndLastAverageSpeed extends RichFlatMapFunction<
            Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>,
            Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> {

        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private transient MapState<Integer, Integer> totalSpeedPerSeg, totalCarsPerSeg, speedPerSeg;
        private transient MapState<String, Integer> carSpeed, carSeg;
        private int averageDelay; // Microsecond

        public AverageSpeedAndLastAverageSpeed(int _averageDelay) {
            this.averageDelay = _averageDelay;
        }

        @Override
        public void flatMap(Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long> input, Collector<Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> out) throws Exception {
            String car_id = input.f2;
            int seg = input.f7, speed = input.f3;
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
                speedPerSeg.put(old_seg, old_seg_avgSpeed);

                out.collect(new Tuple19<>(
                        LinearRoadSource.getSegID(old_seg),
                        input.f1,
                        car_id,
                        old_seg_avgSpeed,
                        input.f4,
                        input.f5,
                        input.f6,
                        old_seg,
                        input.f8,
                        input.f9,
                        input.f10,
                        input.f11,
                        input.f12,
                        input.f13,
                        input.f14,
                        input.f15,
                        LastAverageSpeed_Output,
                        input.f17,
                        input.f18));
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
            speedPerSeg.put(seg, avg_speed);

            if (input.f1 == 4){
                int start_seg = input.f11, end_seg = input.f12;
                if(start_seg > end_seg){
                    int t = start_seg;
                    start_seg = end_seg;
                    end_seg = t;
                }
                int totalTime = 0;
                for(int i = start_seg; i <= end_seg; i++){
                    if(speedPerSeg.contains(i)) {
                        int now_speed = speedPerSeg.get(i);
                        int time;
                        if(now_speed == 0){
                            time = 86400;
                        }else {
                            time = 3600 / now_speed;
                        }
                        totalTime += time;
                    }
                }
                System.out.println("Travel Time Estimation from " + start_seg + " to "  + end_seg + " is " + totalTime + ".");
            }
            out.collect(new Tuple19<>(
                    LinearRoadSource.getSegID(seg),
                    input.f1,
                    car_id,
                    avg_speed,
                    input.f4,
                    input.f5,
                    input.f6,
                    seg,
                    input.f8,
                    input.f9,
                    input.f10,
                    input.f11,
                    input.f12,
                    input.f13,
                    input.f14,
                    input.f15,
                    LastAverageSpeed_Output,
                    input.f17,
                    input.f18));
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
            MapStateDescriptor<String, Integer> descriptor =
                    new MapStateDescriptor<>("average-speed-carseg", String.class, Integer.class);
            carSeg = getRuntimeContext().getMapState(descriptor);
            descriptor = new MapStateDescriptor<>("average-speed-carspeed", String.class, Integer.class);
            carSpeed = getRuntimeContext().getMapState(descriptor);
            MapStateDescriptor<Integer, Integer> descriptor1 = new MapStateDescriptor<>("average-speed-segcars", Integer.class, Integer.class);
            totalCarsPerSeg = getRuntimeContext().getMapState(descriptor1);
            descriptor1 = new MapStateDescriptor<>("average-speed-segspeed", Integer.class, Integer.class);
            totalSpeedPerSeg = getRuntimeContext().getMapState(descriptor1);
            descriptor1 = new MapStateDescriptor<>("last-average-speed-per-seg", Integer.class, Integer.class);
            speedPerSeg = getRuntimeContext().getMapState(descriptor1);
        }
    }

    public static final class CountVehicles extends RichFlatMapFunction<
            Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>,
            Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> {

        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private transient MapState<Integer, Integer> totalCarsPerSeg;
        private transient MapState<String, Integer> carSeg;
        private int averageDelay; // Microsecond

        public CountVehicles(int _averageDelay) {
            this.averageDelay = _averageDelay;
        }

        @Override
        public void flatMap(Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long> input, Collector<Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> out) throws Exception {
            String car_id = input.f2;
            int seg = input.f7;
            if (carSeg.contains(car_id)) {
                int old_seg = carSeg.get(car_id);
                int old_cars = totalCarsPerSeg.get(old_seg);
                totalCarsPerSeg.put(old_seg, old_cars - 1);
                out.collect(new Tuple19<>(
                        LinearRoadSource.getSegID(old_seg),
                        input.f1,
                        car_id,
                        old_cars - 1,
                        input.f4,
                        input.f5,
                        input.f6,
                        old_seg,
                        input.f8,
                        input.f9,
                        input.f10,
                        input.f11,
                        input.f12,
                        input.f13,
                        input.f14,
                        input.f15,
                        CountVehicles_Output,
                        input.f17,
                        input.f18));
            }
            carSeg.put(car_id, seg);
            int old_cars;
            if (!totalCarsPerSeg.contains(seg)) {
                old_cars = 0;
            } else {
                old_cars = totalCarsPerSeg.get(seg);
            }
            totalCarsPerSeg.put(seg, old_cars + 1);
            out.collect(new Tuple19<>(
                    LinearRoadSource.getSegID(seg),
                    input.f1,
                    car_id,
                    old_cars + 1,
                    input.f4,
                    input.f5,
                    input.f6,
                    seg,
                    input.f8,
                    input.f9,
                    input.f10,
                    input.f11,
                    input.f12,
                    input.f13,
                    input.f14,
                    input.f15,
                    CountVehicles_Output,
                    input.f17,
                    input.f18));
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
            MapStateDescriptor<String, Integer> descriptor =
                    new MapStateDescriptor<>("count-vehicle-carseg", String.class, Integer.class);
            carSeg = getRuntimeContext().getMapState(descriptor);
            MapStateDescriptor<Integer, Integer> descriptor1 = new MapStateDescriptor<>("count-vehicle-carspeed", Integer.class, Integer.class);
            totalCarsPerSeg = getRuntimeContext().getMapState(descriptor1);
        }
    }

    public static final class TollNotification extends RichFlatMapFunction<
            Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>,
            Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> {

        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private transient MapState<Integer, Integer> segAverageSpeed, segLastAccident, segCarCounts;
        private int averageDelay; // Microsecond

        public TollNotification(int _averageDelay) {
            this.averageDelay = _averageDelay;
        }

        @Override
        public void flatMap(Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long> input, Collector<Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> out) throws Exception {
            String car_id = input.f2;
            int seg = input.f7, source = input.f16, time = input.f9;
            if (source == AccidentDetection_Output){
                int accident_flag = input.f3;
                if(accident_flag == 1) {
                    segLastAccident.put(seg, time);
                }
                long currentTime = System.currentTimeMillis();
                System.out.println("GT: " + input.f0 + "-" + input.f2 + ", " + currentTime + ", " + (currentTime - input.f17) + ", " + input.f18);
            }else if(source == LastAverageSpeed_Output){
                segAverageSpeed.put(seg, input.f3);
                long currentTime = System.currentTimeMillis();
                System.out.println("GT: " + input.f0 + "-" + input.f2 + ", " + currentTime + ", " + (currentTime - input.f17) + ", " + input.f18);
            }else if(source == CountVehicles_Output){
                segCarCounts.put(seg, input.f3);
            //    long currentTime = System.currentTimeMillis();
            //    System.out.println("GT: " + input.f0 + "-" + input.f2 + ", " + currentTime + ", " + (currentTime - input.f17) + ", " + input.f18);
            // }else if(source == Source_Output){
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
                out.collect(new Tuple19<>(
                        input.f0,
                        input.f1,
                        car_id,
                        price,
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
                        input.f15,
                        TollNotification_Output,
                        input.f17,
                        input.f18));
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

    public static final class TollNotificationAndAccountBalanceAndDailyExpense extends RichFlatMapFunction<
            Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>,
            Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> {

        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private transient MapState<Integer, Integer> segAverageSpeed, segLastAccident, segCarCounts;
        private transient MapState<String, Integer> balancePerCar, dailyExpensePerCar, dayPerCar;
        private int averageDelay; // Microsecond

        public TollNotificationAndAccountBalanceAndDailyExpense(int _averageDelay) {
            this.averageDelay = _averageDelay;
        }

        @Override
        public void flatMap(Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long> input, Collector<Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> out) throws Exception {
            String car_id = input.f2;
            int seg = input.f7, source = input.f16, time = input.f9;
            if (source == AccidentDetection_Output){
                int accident_flag = input.f3;
                if(accident_flag == 1) {
                    segLastAccident.put(seg, time);
                }
                long currentTime = System.currentTimeMillis();
                System.out.println("GT: " + input.f0 + "-" + input.f2 + ", " + currentTime + ", " + (currentTime - input.f17) + ", " + input.f18);
            }else if(source == LastAverageSpeed_Output){
                segAverageSpeed.put(seg, input.f3);
                long currentTime = System.currentTimeMillis();
                System.out.println("GT: " + input.f0 + "-" + input.f2 + ", " + currentTime + ", " + (currentTime - input.f17) + ", " + input.f18);
            }else if(source == CountVehicles_Output) {
                segCarCounts.put(seg, input.f3);
                //    long currentTime = System.currentTimeMillis();
                //    System.out.println("GT: " + input.f0 + "-" + input.f2 + ", " + currentTime + ", " + (currentTime - input.f17) + ", " + input.f18);
                // }else if(source == Source_Output){
                int price = 0, carCounts = 0, averageSpeed = 50;
                if (segCarCounts.contains(seg)) {
                    carCounts = segCarCounts.get(seg);
                }
                if (segAverageSpeed.contains(seg)) {
                    averageSpeed = segAverageSpeed.get(seg);
                }
                price = carCounts * 5 + (100 - averageSpeed);
                if (segLastAccident.contains(seg) && segLastAccident.get(seg) >= time - 300) {
                    price /= 2;
                }
                System.out.println("Toll Notification: car " + car_id + " enter seg " + seg + " price " + price);

                int old_balance = 0;
                if (balancePerCar.contains(car_id)) {
                    old_balance = balancePerCar.get(car_id);
                }
                balancePerCar.put(car_id, old_balance + price);
                if (input.f1 == 2) {
                    int balance = 0;
                    if (balancePerCar.contains(car_id)) {
                        balance = balancePerCar.get(car_id);
                    }
                    System.out.println("Account Balance: car " + car_id + " balance " + balance);
                    long currentTime = System.currentTimeMillis();
                    System.out.println("GT: " + input.f0 + "-" + input.f2 + ", " + currentTime + ", " + (currentTime - input.f17) + ", " + input.f18);
                } else {
                    if (dayPerCar.contains(car_id) && time - dayPerCar.get(car_id) >= 86400) {
                        dayPerCar.put(car_id, time);
                        dailyExpensePerCar.remove(car_id);
                    }
                    int old_expense = 0;
                    if (dailyExpensePerCar.contains(car_id)) {
                        old_expense = dailyExpensePerCar.get(car_id);
                    }
                    dailyExpensePerCar.put(car_id, old_expense + input.f3);
                    long currentTime = System.currentTimeMillis();
                    System.out.println("GT: " + input.f0 + "-" + input.f2 + ", " + currentTime + ", " + (currentTime - input.f17) + ", " + input.f18);
                    // }else if(source == Source_Output){
                    if (input.f1 == 3) {
                        int expense = 0;
                        if (dailyExpensePerCar.contains(car_id)) {
                            expense = dailyExpensePerCar.get(car_id);
                        }
                        System.out.println("Daily Expense: car " + car_id + " expense " + expense);
                    }
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
                    new MapStateDescriptor<>("toll-notification-segspeed", Integer.class, Integer.class);
            segAverageSpeed = getRuntimeContext().getMapState(descriptor);
            descriptor = new MapStateDescriptor<>("toll-notification-segaccident", Integer.class, Integer.class);
            segLastAccident = getRuntimeContext().getMapState(descriptor);
            descriptor = new MapStateDescriptor<>("toll-notification-segcars", Integer.class, Integer.class);
            segCarCounts = getRuntimeContext().getMapState(descriptor);
            MapStateDescriptor<String, Integer> descriptor1 =
                    new MapStateDescriptor<>("account-balance-daily-expense-carbalance", String.class, Integer.class);
            balancePerCar = getRuntimeContext().getMapState(descriptor1);
            descriptor1 = new MapStateDescriptor<>("account-balance-daily-expense-carexpense", String.class, Integer.class);
            dailyExpensePerCar = getRuntimeContext().getMapState(descriptor1);
            descriptor1 = new MapStateDescriptor<>("account-balance-daily-expense-carday", String.class, Integer.class);
            dayPerCar = getRuntimeContext().getMapState(descriptor1);
        }
    }

    public static final class AccountBalance extends RichFlatMapFunction<
            Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>,
            Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> {

        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private transient MapState<String, Integer> balancePerCar;
        private int averageDelay; // Microsecond

        public AccountBalance(int _averageDelay) {
            this.averageDelay = _averageDelay;
        }

        @Override
        public void flatMap(Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long> input, Collector<Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> out) throws Exception {
            String car_id = input.f2;
            int source = input.f16;
            if (source == TollNotification_Output){
                int old_balance = 0;
                if(balancePerCar.contains(car_id)){
                    old_balance = balancePerCar.get(car_id);
                }
                balancePerCar.put(car_id, old_balance + input.f3);
                out.collect(new Tuple19<>(
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
                        input.f15,
                        AccountBalance_Output,
                        input.f17,
                        input.f18));
            // }else if(source == Source_Output){
                if (input.f1 == 2) {
                    int balance = 0;
                    if (balancePerCar.contains(car_id)){
                        balance = balancePerCar.get(car_id);
                    }
                    System.out.println("Account Balance: car " + car_id + " balance " + balance);
                    long currentTime = System.currentTimeMillis();
                    System.out.println("GT: " + input.f0 + "-" + input.f2 + ", " + currentTime + ", " + (currentTime - input.f17) + ", " + input.f18);
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
            MapStateDescriptor<String, Integer> descriptor =
                    new MapStateDescriptor<>("account-balance-carbalance", String.class, Integer.class);
            balancePerCar = getRuntimeContext().getMapState(descriptor);
        }
    }

    public static final class AccountBalanceAndDailyExpense extends RichFlatMapFunction<
            Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>,
            Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> {

        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private transient MapState<String, Integer> balancePerCar, dailyExpensePerCar, dayPerCar;
        private int averageDelay; // Microsecond

        public AccountBalanceAndDailyExpense(int _averageDelay) {
            this.averageDelay = _averageDelay;
        }

        @Override
        public void flatMap(Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long> input, Collector<Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> out) throws Exception {
            String car_id = input.f2;
            int source = input.f16, time = input.f9;;
            if (source == TollNotification_Output){
                int old_balance = 0;
                if(balancePerCar.contains(car_id)){
                    old_balance = balancePerCar.get(car_id);
                }
                balancePerCar.put(car_id, old_balance + input.f3);
                if (input.f1 == 2) {
                    int balance = 0;
                    if (balancePerCar.contains(car_id)){
                        balance = balancePerCar.get(car_id);
                    }
                    System.out.println("Account Balance: car " + car_id + " balance " + balance);
                    long currentTime = System.currentTimeMillis();
                    System.out.println("GT: " + input.f0 + "-" + input.f2 + ", " + currentTime + ", " + (currentTime - input.f17) + ", " + input.f18);
                }else{
                    if(dayPerCar.contains(car_id) && time - dayPerCar.get(car_id) >= 86400){
                        dayPerCar.put(car_id, time);
                        dailyExpensePerCar.remove(car_id);
                    }
                    int old_expense = 0;
                    if(dailyExpensePerCar.contains(car_id)){
                        old_expense = dailyExpensePerCar.get(car_id);
                    }
                    dailyExpensePerCar.put(car_id, old_expense + input.f3);
                    long currentTime = System.currentTimeMillis();
                    System.out.println("GT: " + input.f0 + "-" + input.f2 + ", " + currentTime + ", " + (currentTime - input.f17) + ", " + input.f18);
                    // }else if(source == Source_Output){
                    if (input.f1 == 3) {
                        int expense = 0;
                        if (dailyExpensePerCar.contains(car_id)){
                            expense = dailyExpensePerCar.get(car_id);
                        }
                        System.out.println("Daily Expense: car " + car_id + " expense " + expense);
                    }
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
            MapStateDescriptor<String, Integer> descriptor =
                    new MapStateDescriptor<>("account-balance-daily-expense-carbalance", String.class, Integer.class);
            balancePerCar = getRuntimeContext().getMapState(descriptor);
            descriptor = new MapStateDescriptor<>("account-balance-daily-expense-carexpense", String.class, Integer.class);
            dailyExpensePerCar = getRuntimeContext().getMapState(descriptor);
            descriptor = new MapStateDescriptor<>("account-balance-daily-expense-carday", String.class, Integer.class);
            dayPerCar = getRuntimeContext().getMapState(descriptor);
        }
    }

    public static final class DailyExpense extends RichFlatMapFunction<
            Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>,
            Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> {

        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private transient MapState<String, Integer> dailyExpensePerCar, dayPerCar;
        private int averageDelay; // Microsecond

        public DailyExpense(int _averageDelay) {
            this.averageDelay = _averageDelay;
        }

        @Override
        public void flatMap(Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long> input, Collector<Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> out) throws Exception {
            String car_id = input.f2;
            int source = input.f16, time = input.f9;
            if (source == AccountBalance_Output){
                if(dayPerCar.contains(car_id) && time - dayPerCar.get(car_id) >= 86400){
                    dayPerCar.put(car_id, time);
                    dailyExpensePerCar.remove(car_id);
                }
                int old_expense = 0;
                if(dailyExpensePerCar.contains(car_id)){
                    old_expense = dailyExpensePerCar.get(car_id);
                }
                dailyExpensePerCar.put(car_id, old_expense + input.f3);
                long currentTime = System.currentTimeMillis();
                System.out.println("GT: " + input.f0 + "-" + input.f2 + ", " + currentTime + ", " + (currentTime - input.f17) + ", " + input.f18);
            // }else if(source == Source_Output){
                if (input.f1 == 3) {
                    int expense = 0;
                    if (dailyExpensePerCar.contains(car_id)){
                        expense = dailyExpensePerCar.get(car_id);
                    }
                    System.out.println("Daily Expense: car " + car_id + " expense " + expense);
                    currentTime = System.currentTimeMillis();
                    System.out.println("GT: " + input.f0 + "-" + input.f2 + ", " + currentTime + ", " + (currentTime - input.f17) + ", " + input.f18);
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
            MapStateDescriptor<String, Integer> descriptor =
                    new MapStateDescriptor<>("daily-expense-carexpense", String.class, Integer.class);
            dailyExpensePerCar = getRuntimeContext().getMapState(descriptor);
            descriptor = new MapStateDescriptor<>("daily-expense-carday", String.class, Integer.class);
            dayPerCar = getRuntimeContext().getMapState(descriptor);
        }
    }

}
