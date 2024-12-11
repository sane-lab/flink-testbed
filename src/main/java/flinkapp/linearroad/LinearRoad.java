package flinkapp.linearroad;

import Nexmark.sources.Util;
import common.FastZipfGenerator;
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
import org.apache.flink.util.MathUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

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

        DataStreamSource<Tuple2<String, LinearRoadRecord>> source = env
                .addSource(new LinearRoadSource(
                        params.get("file_name", "/home/samza/LR_data/3hr.txt"),
                        params.getLong("warmup_time", 30L) * 1000,
                        params.getLong("warmup_rate", 1500L),
                        params.getLong("skip_interval", 0L) * 20,
                        params.getDouble("input_rate_factor", 1.0),
                        params.getInt("mp2", 8),
                        params.getDouble("skew_factor", 0.0)
                ))
                .setParallelism(params.getInt("p1", 1));

        // Accident Detection
        DataStream<Tuple2<String, LinearRoadRecord>> afterAccidentDetection = source
                .keyBy(0)
                .flatMap(new AccidentDetection(params.getInt("op2Delay", 1000)))
                .disableChaining()
                .name("Accident Detection")
                .uid("op2")
                .setParallelism(params.getInt("p2", 1))
                .setMaxParallelism(params.getInt("mp2", 8))
                .slotSharingGroup("g2");

        // Combine Average Speed and Last Average Speed computation
        DataStream<Tuple2<String, LinearRoadRecord>> afterAverageSpeed = source
                .keyBy(0)
                .flatMap(new AverageSpeedAndLastAverageSpeed(params.getInt("op3Delay", 1000), params.getInt("payload", 0)))
                .disableChaining()
                .name("Average Speed and Last Average Speed")
                .uid("op3")
                .setParallelism(params.getInt("p3", 1))
                .setMaxParallelism(params.getInt("mp3", 8))
                .slotSharingGroup("g3");

        // Count Vehicles operator
        DataStream<Tuple2<String, LinearRoadRecord>> afterCountVehicles = source
                .keyBy(0)
                .flatMap(new CountVehicles(params.getInt("op4Delay", 1000)))
                .disableChaining()
                .name("Count Vehicles")
                .uid("op4")
                .setParallelism(params.getInt("p4", 1))
                .setMaxParallelism(params.getInt("mp4", 8))
                .slotSharingGroup("g4");

        // Toll Notification and Account Balance & Daily Expense
        // Here we unify the three resulting streams: afterAccidentDetection, afterAverageSpeed, and afterCountVehicles.
        // The unioned stream is keyed and processed by TollNotificationAndAccountBalanceAndDailyExpense.
        DataStream<Tuple2<String, LinearRoadRecord>> afterTollNotification = afterAccidentDetection
                .union(afterAverageSpeed)
                .union(afterCountVehicles)
                .keyBy(0)
                .flatMap(new TollNotificationAndAccountBalanceAndDailyExpense(params.getInt("op5Delay", 1000), params.getInt("payload", 0)))
                .disableChaining()
                .name("Toll Notification and Account Balance")
                .uid("op5")
                .setParallelism(params.getInt("p5", 1))
                .setMaxParallelism(params.getInt("mp5", 8))
                .slotSharingGroup("g5");


        env.execute();
    }

    public static class LinearRoadRecord {
        private String segID;
        private Integer type;
        private String carID;
        private Integer speed;
        private Integer xway;
        private Integer lane;
        private Integer dir;
        private Integer seg;
        private Integer pos;
        private Integer time;
        private Integer queryID;
        private Integer qStart;
        private Integer qEnd;
        private Integer qDayOfWeek;
        private Integer qMinutes;
        private Integer qDay;
        private Integer outputOperator;
        private Long arrivalTime;
        private Long tupleNumber;

        public LinearRoadRecord(String segID,
                                Integer type,
                                String carID,
                                Integer speed,
                                Integer xway,
                                Integer lane,
                                Integer dir,
                                Integer seg,
                                Integer pos,
                                Integer time,
                                Integer queryID,
                                Integer qStart,
                                Integer qEnd,
                                Integer qDayOfWeek,
                                Integer qMinutes,
                                Integer qDay,
                                Integer outputOperator,
                                Long arrivalTime,
                                Long tupleNumber) {
            this.segID = segID;
            this.type = type;
            this.carID = carID;
            this.speed = speed;
            this.xway = xway;
            this.lane = lane;
            this.dir = dir;
            this.seg = seg;
            this.pos = pos;
            this.time = time;
            this.queryID = queryID;
            this.qStart = qStart;
            this.qEnd = qEnd;
            this.qDayOfWeek = qDayOfWeek;
            this.qMinutes = qMinutes;
            this.qDay = qDay;
            this.outputOperator = outputOperator;
            this.arrivalTime = arrivalTime;
            this.tupleNumber = tupleNumber;
        }

        // Getters
        public String getSegID() {
            return segID;
        }

        public Integer getType() {
            return type;
        }

        public String getCarID() {
            return carID;
        }

        public Integer getSpeed() {
            return speed;
        }

        public Integer getXway() {
            return xway;
        }

        public Integer getLane() {
            return lane;
        }

        public Integer getDir() {
            return dir;
        }

        public Integer getSeg() {
            return seg;
        }

        public Integer getPos() {
            return pos;
        }

        public Integer getTime() {
            return time;
        }

        public Integer getQueryID() {
            return queryID;
        }

        public Integer getQStart() {
            return qStart;
        }

        public Integer getQEnd() {
            return qEnd;
        }

        public Integer getQDayOfWeek() {
            return qDayOfWeek;
        }

        public Integer getQMinutes() {
            return qMinutes;
        }

        public Integer getQDay() {
            return qDay;
        }

        public Integer getOutputOperator() {
            return outputOperator;
        }

        public Long getArrivalTime() {
            return arrivalTime;
        }

        public Long getTupleNumber() {
            return tupleNumber;
        }

        // Setters (optional, only if you need to modify fields after construction)
        // public void setSegID(String segID) { this.segID = segID; }
        // public void setType(Integer type) { this.type = type; }
        // ... add setters for other fields as needed
    }


    public static class LinearRoadSource extends RichParallelSourceFunction<Tuple2<String, LinearRoadRecord>> {
        private volatile boolean running = true;

        // Field indices as constants for clarity
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
        private final double input_rate_factor;
        private FastZipfGenerator fastZipfGenerator;
        private final boolean isSkewed;
        private final Map<Integer, List<String>> keyGroupMapping = new HashMap<>();

        public static String getSegID(int seg) {
            return "A" + seg;
        }

        public static String getCarID(int car_ID) {
            return String.format("A%06d", car_ID);
        }

        public LinearRoadSource(String FILE, long warmup, long warmup_rate, long skipCount, double input_rate_factor, int maxParallelism, double zipfSkew) {
            this.FILE = FILE;
            this.warmup = warmup;
            this.warmp_rate = warmup_rate;
            this.skipCount = skipCount;
            this.input_rate_factor = input_rate_factor;
            this.isSkewed = (zipfSkew > 1e-10);
            if (isSkewed) {
                for (int i = 0; i < 1000; i++) {
                    String key = getSegID(i);
                    int keygroup = MathUtils.murmurHash(key.hashCode()) % maxParallelism;
                    List<String> keys = keyGroupMapping.computeIfAbsent(keygroup, t -> new ArrayList<>());
                    keys.add(key);
                }
                this.fastZipfGenerator = new FastZipfGenerator(maxParallelism, zipfSkew, 0, 114514);
            }
        }

        private String getSubKeySetChar(int cur, List<String> subKeySet) {
            return subKeySet.get(cur % subKeySet.size());
        }

        @Override
        public void run(SourceContext<Tuple2<String, LinearRoadRecord>> ctx) throws Exception {
            String sCurrentLine;
            List<String> subKeySet;
            FileReader stream = null;
            BufferedReader br = null;
            int count = 0, counter = 0, input_factor_count = 0;
            int sleepCnt = 0, noRecSleepCnt = 0;

            long startTime = System.currentTimeMillis();
            System.out.println("Warmup start at: " + startTime);

            // Warmup phase
            while (System.currentTimeMillis() - startTime < warmup) {
                long emitStartTime = System.currentTimeMillis();
                for (int i = 0; i < warmp_rate * input_rate_factor / 20; i++) {
                    int car_id = count % 100000; //1000000;
                    String key = getCarID(car_id);
                    if (isSkewed) {
                        int selectedKeygroup = fastZipfGenerator.next();
                        subKeySet = keyGroupMapping.get(selectedKeygroup);
                        key = getSubKeySetChar(count, subKeySet);
                    }
                    int seg = count % 100;
                    String seg_ID = getSegID(seg);

                    ctx.collect(new Tuple2<>(key, new LinearRoadRecord(
                            seg_ID, 0, key, 0, 0, 0, 0, seg, 0, 0,
                            0, 0, 0, 0, 0, 0, 0,
                            System.currentTimeMillis(),
                            (long) count)
                    ));
                    count++;
                }
                Util.pause(emitStartTime);
            }

            try {
                stream = new FileReader(FILE);
                br = new BufferedReader(stream);

                long start = System.currentTimeMillis();
                long cur;

                while ((sCurrentLine = br.readLine()) != null) {
                    if (sCurrentLine.equals("END")) {
                        sleepCnt++;
                        if (counter == 0) {
                            noRecSleepCnt++;
                            System.out.println("no record in this sleep !" + noRecSleepCnt);
                        }

                        if (sleepCnt <= skipCount) {
                            for (int i = 0; i < warmp_rate * input_rate_factor / 20; i++) {
                                int car_id = count % 1000000;
                                String key = getCarID(car_id);
                                if (isSkewed) {
                                    int selectedKeygroup = fastZipfGenerator.next();
                                    subKeySet = keyGroupMapping.get(selectedKeygroup);
                                    key = getSubKeySetChar(count, subKeySet);
                                }
                                int seg = count % 100;
                                String seg_ID = getSegID(seg);

                                ctx.collect(new Tuple2<>(key, new LinearRoadRecord(
                                        seg_ID, 0, key, 0, 0, 0, 0, seg, 0, 0,
                                        0, 0, 0, 0, 0, 0, 0,
                                        System.currentTimeMillis(),
                                        (long) count)
                                ));
                                count++;
                            }
                        }
                        System.out.println("!! C: " + counter + " A: " + input_factor_count + " F: " + input_rate_factor);
                        counter = 0;
                        input_factor_count = 0;
                        cur = System.currentTimeMillis();
                        if (cur < sleepCnt * 50 + start) {
                            Thread.sleep((sleepCnt * 50 + start) - cur);
                        } else {
                            System.out.println("rate exceeds 50 ms.");
                        }
                        continue;
                    }

                    String[] fields = sCurrentLine.split(",");
                    if (fields.length < 10) {
                        continue;
                    }

                    if (sleepCnt > skipCount) {
                        Long ts = System.currentTimeMillis();
                        counter++;
                        int seg = Integer.parseInt(fields[Seg - 1]);
                        int car_id = Integer.parseInt(fields[Car_ID - 1]);
                        String key = getCarID(car_id);
                        if (isSkewed) {
                            int selectedKeygroup = fastZipfGenerator.next();
                            subKeySet = keyGroupMapping.get(selectedKeygroup);
                            key = getSubKeySetChar(count, subKeySet);
                        }

                        int round_factor = (int) (input_rate_factor + 1e-9);
                        for (int rep = 0; rep < round_factor; rep++) {
                            ctx.collect(new Tuple2<>(key, new LinearRoadRecord(
                                    getSegID(seg),
                                    Integer.parseInt(fields[0]),
                                    key,
                                    Integer.parseInt(fields[2]),
                                    Integer.parseInt(fields[3]),
                                    Integer.parseInt(fields[4]),
                                    Integer.parseInt(fields[5]),
                                    Integer.parseInt(fields[6]),
                                    Integer.parseInt(fields[7]),
                                    Integer.parseInt(fields[8]),
                                    Integer.parseInt(fields[9]),
                                    Integer.parseInt(fields[10]),
                                    Integer.parseInt(fields[11]),
                                    Integer.parseInt(fields[12]),
                                    Integer.parseInt(fields[13]),
                                    Integer.parseInt(fields[14]),
                                    0,
                                    ts,
                                    (long) count)
                            ));
                            count++;
                        }

                        int new_input = (int) ((input_rate_factor - round_factor) * counter + 1e-9);
                        if (new_input > input_factor_count) {
                            input_factor_count++;
                            ctx.collect(new Tuple2<>(key, new LinearRoadRecord(
                                    getSegID(seg),
                                    Integer.parseInt(fields[0]),
                                    key,
                                    Integer.parseInt(fields[2]),
                                    Integer.parseInt(fields[3]),
                                    Integer.parseInt(fields[4]),
                                    Integer.parseInt(fields[5]),
                                    Integer.parseInt(fields[6]),
                                    Integer.parseInt(fields[7]),
                                    Integer.parseInt(fields[8]),
                                    Integer.parseInt(fields[9]),
                                    Integer.parseInt(fields[10]),
                                    Integer.parseInt(fields[11]),
                                    Integer.parseInt(fields[12]),
                                    Integer.parseInt(fields[13]),
                                    Integer.parseInt(fields[14]),
                                    0,
                                    ts,
                                    (long) count)
                            ));
                            count++;
                        }
                    }
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

    public static class DelayUtil {
        private static final RandomDataGenerator randomGen = new RandomDataGenerator();

        public static void delay(long intervalMicroseconds) {
            Double ranN = randomGen.nextGaussian(intervalMicroseconds, 1) * 1000;
            long delayNanos = ranN.intValue();
            if (delayNanos < 0) delayNanos = intervalMicroseconds * 1000;
            long start = System.nanoTime();
            while (System.nanoTime() - start < delayNanos) {
                // Busy waiting
            }
        }
    }


    public static final class AccidentDetection extends RichFlatMapFunction<Tuple2<String, LinearRoadRecord>, Tuple2<String, LinearRoadRecord>> {

        private static final int ACCIDENT_DETECTION_OUTPUT = 999; // Arbitrary identifier for accident detection output

        private transient MapState<String, Integer> carLastPos;
        private transient MapState<String, Integer> carStayLength;
        private int averageDelay; // in microseconds

        public AccidentDetection(int averageDelay) {
            this.averageDelay = averageDelay;
        }

        @Override
        public void flatMap(Tuple2<String, LinearRoadRecord> input, Collector<Tuple2<String, LinearRoadRecord>> out) throws Exception {
            LinearRoadRecord input_record = input.f1;
            String car_id = input_record.getCarID();
            int pos = input_record.getPos();
            DelayUtil.delay(averageDelay);
            // Check if the car's position is unchanged
            if (carLastPos.contains(car_id) && carLastPos.get(car_id) == pos) {
                int stayLength = carStayLength.get(car_id) + 1;
                if (stayLength >= 4) {
                    // Accident scenario: set speed to 1
                    out.collect(new Tuple2<>(car_id, new LinearRoadRecord(
                            input_record.getSegID(),
                            input_record.getType(),
                            car_id,
                            1, // speed = 1 to indicate accident
                            input_record.getXway(),
                            input_record.getLane(),
                            input_record.getDir(),
                            input_record.getSeg(),
                            input_record.getPos(),
                            input_record.getTime(),
                            input_record.getQueryID(),
                            input_record.getQStart(),
                            input_record.getQEnd(),
                            input_record.getQDayOfWeek(),
                            input_record.getQMinutes(),
                            input_record.getQDay(),
                            ACCIDENT_DETECTION_OUTPUT, // updated operator output
                            input_record.getArrivalTime(),
                            input_record.getTupleNumber())
                    ));
                } else {
                    // No accident yet: speed = 0
                    out.collect(new Tuple2<>(car_id, new LinearRoadRecord(
                            input_record.getSegID(),
                            input_record.getType(),
                            car_id,
                            0,
                            input_record.getXway(),
                            input_record.getLane(),
                            input_record.getDir(),
                            input_record.getSeg(),
                            input_record.getPos(),
                            input_record.getTime(),
                            input_record.getQueryID(),
                            input_record.getQStart(),
                            input_record.getQEnd(),
                            input_record.getQDayOfWeek(),
                            input_record.getQMinutes(),
                            input_record.getQDay(),
                            ACCIDENT_DETECTION_OUTPUT,
                            input_record.getArrivalTime(),
                            input_record.getTupleNumber())
                    ));
                }
                carStayLength.put(car_id, stayLength);
            } else {
                // First occurrence or the car moved: reset stay length to 1, speed = 0
                out.collect(new Tuple2<>(car_id, new LinearRoadRecord(
                        input_record.getSegID(),
                        input_record.getType(),
                        car_id,
                        0, // no accident, speed = 0
                        input_record.getXway(),
                        input_record.getLane(),
                        input_record.getDir(),
                        input_record.getSeg(),
                        input_record.getPos(),
                        input_record.getTime(),
                        input_record.getQueryID(),
                        input_record.getQStart(),
                        input_record.getQEnd(),
                        input_record.getQDayOfWeek(),
                        input_record.getQMinutes(),
                        input_record.getQDay(),
                        ACCIDENT_DETECTION_OUTPUT,
                        input_record.getArrivalTime(),
                        input_record.getTupleNumber())
                ));
                carLastPos.put(car_id, pos);
                carStayLength.put(car_id, 1);
            }
            // Introduce a processing delay for simulation purposes

        }

        @Override
        public void open(Configuration config) {
            MapStateDescriptor<String, Integer> posDescriptor =
                    new MapStateDescriptor<>("accident-detection-pos", String.class, Integer.class);
            carLastPos = getRuntimeContext().getMapState(posDescriptor);

            MapStateDescriptor<String, Integer> stayDescriptor =
                    new MapStateDescriptor<>("accident-detection-stay", String.class, Integer.class);
            carStayLength = getRuntimeContext().getMapState(stayDescriptor);
        }
    }

    public static final class AverageSpeedAndLastAverageSpeed extends RichFlatMapFunction<Tuple2<String, LinearRoadRecord>, Tuple2<String, LinearRoadRecord>> {

        public static class SegmentMetrics implements Serializable {
            private int totalSpeed;
            private int totalCars;
            private int avgSpeed;

            public SegmentMetrics() {
                this.totalSpeed = 0;
                this.totalCars = 0;
                this.avgSpeed = 0;
            }

            public int getTotalSpeed() {
                return totalSpeed;
            }

            public void setTotalSpeed(int totalSpeed) {
                this.totalSpeed = totalSpeed;
            }

            public int getTotalCars() {
                return totalCars;
            }

            public void setTotalCars(int totalCars) {
                this.totalCars = totalCars;
            }

            public int getAvgSpeed() {
                return avgSpeed;
            }

            public void setAvgSpeed(int avgSpeed) {
                this.avgSpeed = avgSpeed;
            }
        }

        public static class CarMetrics implements Serializable {
            private int speed;
            private int seg;
            private String extraLoad;

            public CarMetrics() {
                this.speed = 0;
                this.seg = -1; // indicates no known segment yet
                this.extraLoad = null;
            }

            public int getSpeed() {
                return speed;
            }

            public void setSpeed(int speed) {
                this.speed = speed;
            }

            public int getSeg() {
                return seg;
            }

            public void setSeg(int seg) {
                this.seg = seg;
            }

            public String getExtraLoad() {
                return extraLoad;
            }

            public void setExtraLoad(String extraLoad) {
                this.extraLoad = extraLoad;
            }
        }

        private transient MapState<Integer, SegmentMetrics> segmentStateMap;
        private transient MapState<String, CarMetrics> carStateMap;

        private final int averageDelay; // Microseconds
        private final String payload;
        private final boolean payloadFlag;

        public AverageSpeedAndLastAverageSpeed(int averageDelay, int payloadLength) {
            this.averageDelay = averageDelay;
            if (payloadLength > 0) {
                this.payloadFlag = true;
                this.payload = new String(new char[payloadLength]).replace("\0", "a");
            } else {
                this.payloadFlag = false;
                this.payload = "";
            }
        }

        @Override
        public void flatMap(Tuple2<String, LinearRoadRecord> input, Collector<Tuple2<String, LinearRoadRecord>> out) throws Exception {
            LinearRoadRecord inputRecord = input.f1;
            String carId = inputRecord.getCarID();
            int seg = inputRecord.getSeg();
            int speed = inputRecord.getSpeed();

            // Retrieve or initialize car and segment states
            CarMetrics carMetrics = carStateMap.contains(carId) ? carStateMap.get(carId) : new CarMetrics();
            SegmentMetrics segMetrics = segmentStateMap.contains(seg) ? segmentStateMap.get(seg) : new SegmentMetrics();

            // If this car was previously associated with a different segment, update that old segment's metrics
            if (carMetrics.getSeg() != -1 && carMetrics.getSeg() != seg) {
                int oldSeg = carMetrics.getSeg();
                CarMetrics oldCarMetrics = carMetrics; // already have

                // Retrieve old segment state
                SegmentMetrics oldSegMetrics = segmentStateMap.contains(oldSeg) ? segmentStateMap.get(oldSeg) : new SegmentMetrics();

                // Remove old car's contribution
                oldSegMetrics.setTotalCars(oldSegMetrics.getTotalCars() - 1);
                oldSegMetrics.setTotalSpeed(oldSegMetrics.getTotalSpeed() - oldCarMetrics.getSpeed());

                int oldCars = oldSegMetrics.getTotalCars();
                int oldTotalSpeed = oldSegMetrics.getTotalSpeed();
                int oldSegAvgSpeed = (oldCars > 0) ? (oldTotalSpeed / oldCars) : 0;
                oldSegMetrics.setAvgSpeed(oldSegAvgSpeed);

                // Store updated old segment state
                segmentStateMap.put(oldSeg, oldSegMetrics);
            }

            // Update the current car's position and speed
            carMetrics.setSeg(seg);
            carMetrics.setSpeed(speed);

            // Optionally store the payload for the current car
            if (payloadFlag) {
                carMetrics.setExtraLoad(payload);
            }

            // Update metrics for the current segment
            segMetrics.setTotalCars(segMetrics.getTotalCars() + 1);
            segMetrics.setTotalSpeed(segMetrics.getTotalSpeed() + speed);
            int avgSpeed = segMetrics.getTotalSpeed() / segMetrics.getTotalCars();
            segMetrics.setAvgSpeed(avgSpeed);

            // Store updated states
            carStateMap.put(carId, carMetrics);
            segmentStateMap.put(seg, segMetrics);

            // If the operator type is 4, perform travel time estimation
            if (inputRecord.getType() == 4) {
                int startSeg = inputRecord.getQStart();
                int endSeg = inputRecord.getQEnd();
                if (startSeg > endSeg) {
                    int temp = startSeg;
                    startSeg = endSeg;
                    endSeg = temp;
                }

                int totalTime = 0;
                for (int i = startSeg; i <= endSeg; i++) {
                    if (segmentStateMap.contains(i)) {
                        SegmentMetrics sMetrics = segmentStateMap.get(i);
                        int currentSpeed = sMetrics.getAvgSpeed();
                        int segmentTime = (currentSpeed == 0) ? 86400 : (3600 / currentSpeed);
                        totalTime += segmentTime;
                    } else {
                        // If no info, assume default speed of 50 or handle accordingly
                        int segmentTime = 3600 / 50; // or another default handling
                        totalTime += segmentTime;
                    }
                }
                System.out.println("Travel Time Estimation from " + startSeg + " to " + endSeg + " is " + totalTime + ".");
            }

            // Introduce processing delay
            DelayUtil.delay(averageDelay);

            // Emit updated record with the last average speed
            out.collect(new Tuple2<>(carId, new LinearRoadRecord(
                    inputRecord.getSegID(),
                    inputRecord.getType(),
                    carId,
                    avgSpeed,
                    inputRecord.getXway(),
                    inputRecord.getLane(),
                    inputRecord.getDir(),
                    seg,
                    inputRecord.getPos(),
                    inputRecord.getTime(),
                    inputRecord.getQueryID(),
                    inputRecord.getQStart(),
                    inputRecord.getQEnd(),
                    inputRecord.getQDayOfWeek(),
                    inputRecord.getQMinutes(),
                    inputRecord.getQDay(),
                    LastAverageSpeed_Output,
                    inputRecord.getArrivalTime(),
                    inputRecord.getTupleNumber())
            ));
        }

        @Override
        public void open(Configuration config) {
            MapStateDescriptor<Integer, SegmentMetrics> segmentDesc =
                    new MapStateDescriptor<>("segment-metrics", Integer.class, SegmentMetrics.class);
            segmentStateMap = getRuntimeContext().getMapState(segmentDesc);

            MapStateDescriptor<String, CarMetrics> carDesc =
                    new MapStateDescriptor<>("car-metrics", String.class, CarMetrics.class);
            carStateMap = getRuntimeContext().getMapState(carDesc);
        }
    }

    public static final class CountVehicles extends RichFlatMapFunction<Tuple2<String, LinearRoadRecord>, Tuple2<String, LinearRoadRecord>> {

        private transient MapState<Integer, Integer> totalCarsPerSeg;
        private transient MapState<String, Integer> carSeg;
        private final int averageDelay; // Microseconds

        public CountVehicles(int averageDelay) {
            this.averageDelay = averageDelay;
        }

        @Override
        public void flatMap(Tuple2<String, LinearRoadRecord> input, Collector<Tuple2<String, LinearRoadRecord>> out) throws Exception {
            LinearRoadRecord input_record = input.f1;
            String carId = input_record.getCarID();
            int seg = input_record.getSeg();

            // If this car was previously associated with a different segment, decrement that segment's count
            if (carSeg.contains(carId)) {
                int oldSeg = carSeg.get(carId);
                int oldCars = totalCarsPerSeg.get(oldSeg);
                totalCarsPerSeg.put(oldSeg, oldCars - 1);
            }

            // Update the car's current segment
            carSeg.put(carId, seg);

            // Update the number of cars in the current segment
            int oldCars = totalCarsPerSeg.contains(seg) ? totalCarsPerSeg.get(seg) : 0;
            totalCarsPerSeg.put(seg, oldCars + 1);

            // Introduce the delay to simulate processing time
            DelayUtil.delay(averageDelay);

            // Emit a record reflecting the updated count
            out.collect(new Tuple2<>(carId, new LinearRoadRecord(
                    input_record.getSegID(),
                    input_record.getType(),
                    carId,
                    oldCars + 1, // Using the speed field as vehicle count here if consistent with your schema
                    input_record.getXway(),
                    input_record.getLane(),
                    input_record.getDir(),
                    seg,
                    input_record.getPos(),
                    input_record.getTime(),
                    input_record.getQueryID(),
                    input_record.getQStart(),
                    input_record.getQEnd(),
                    input_record.getQDayOfWeek(),
                    input_record.getQMinutes(),
                    input_record.getQDay(),
                    CountVehicles_Output,  // A constant that you must define or reference
                    input_record.getArrivalTime(),
                    input_record.getTupleNumber())
            ));
        }

        @Override
        public void open(Configuration config) {
            MapStateDescriptor<String, Integer> carSegDesc =
                    new MapStateDescriptor<>("count-vehicle-carseg", String.class, Integer.class);
            carSeg = getRuntimeContext().getMapState(carSegDesc);

            MapStateDescriptor<Integer, Integer> totalCarsPerSegDesc =
                    new MapStateDescriptor<>("count-vehicle-totalcars", Integer.class, Integer.class);
            totalCarsPerSeg = getRuntimeContext().getMapState(totalCarsPerSegDesc);
        }
    }

    public static final class TollNotificationAndAccountBalanceAndDailyExpense
            extends RichFlatMapFunction<Tuple2<String, LinearRoadRecord>, Tuple2<String, LinearRoadRecord>> {

        public static class SegmentState implements Serializable {
            private Integer averageSpeed;
            private Integer lastAccident;
            private Integer carCounts;

            public SegmentState() {
                this.averageSpeed = 50; // default average speed if not set
                this.lastAccident = null;
                this.carCounts = 0;
            }

            public Integer getAverageSpeed() {
                return averageSpeed;
            }

            public void setAverageSpeed(Integer averageSpeed) {
                this.averageSpeed = averageSpeed;
            }

            public Integer getLastAccident() {
                return lastAccident;
            }

            public void setLastAccident(Integer lastAccident) {
                this.lastAccident = lastAccident;
            }

            public Integer getCarCounts() {
                return carCounts;
            }

            public void setCarCounts(Integer carCounts) {
                this.carCounts = carCounts;
            }
        }

        public static class CarState implements Serializable {
            private Integer balance;
            private Integer dailyExpense;
            private Integer lastDay;
            private String extraLoad;

            public CarState() {
                this.balance = 0;
                this.dailyExpense = 0;
                this.lastDay = 0;
                this.extraLoad = null;
            }

            public Integer getBalance() {
                return balance;
            }

            public void setBalance(Integer balance) {
                this.balance = balance;
            }

            public Integer getDailyExpense() {
                return dailyExpense;
            }

            public void setDailyExpense(Integer dailyExpense) {
                this.dailyExpense = dailyExpense;
            }

            public Integer getLastDay() {
                return lastDay;
            }

            public void setLastDay(Integer lastDay) {
                this.lastDay = lastDay;
            }

            public String getExtraLoad() {
                return extraLoad;
            }

            public void setExtraLoad(String extraLoad) {
                this.extraLoad = extraLoad;
            }
        }

        private transient MapState<Integer, SegmentState> segmentStateMap;
        private transient MapState<String, CarState> carStateMap;


        private final int averageDelay; // Microseconds
        private final String payload;
        private final boolean payloadFlag;

        public TollNotificationAndAccountBalanceAndDailyExpense(int averageDelay, int payloadLength) {
            this.averageDelay = averageDelay;
            if (payloadLength > 0) {
                this.payloadFlag = true;
                this.payload = new String(new char[payloadLength]).replace("\0", "a");
            } else {
                this.payloadFlag = false;
                this.payload = "";
            }
        }

        @Override
        public void flatMap(Tuple2<String, LinearRoadRecord> input, Collector<Tuple2<String, LinearRoadRecord>> out) throws Exception {
            LinearRoadRecord inputRecord = input.f1;
            String carId = inputRecord.getCarID();
            int seg = inputRecord.getSeg();
            int source = inputRecord.getOutputOperator();
            int time = inputRecord.getTime();

            long currentTime = System.currentTimeMillis();
            long latency = currentTime - inputRecord.getArrivalTime();

            // Retrieve or initialize segment state
            SegmentState segState = segmentStateMap.contains(seg) ? segmentStateMap.get(seg) : new SegmentState();

            // Retrieve or initialize car state
            CarState cState = carStateMap.contains(carId) ? carStateMap.get(carId) : new CarState();

            // Process record based on source operator
            if (source == AccidentDetection_Output) {
                int accidentFlag = inputRecord.getSpeed();
                if (accidentFlag == 1) {
                    segState.setLastAccident(time);
                }
                System.out.println("GT: " + inputRecord.getSegID() + "-" + carId + ", " + currentTime + ", " + latency + ", " + inputRecord.getTupleNumber());

            } else if (source == LastAverageSpeed_Output) {
                segState.setAverageSpeed(inputRecord.getSpeed());
                System.out.println("GT: " + inputRecord.getSegID() + "-" + carId + ", " + currentTime + ", " + latency + ", " + inputRecord.getTupleNumber());

            } else if (source == CountVehicles_Output) {
                segState.setCarCounts(inputRecord.getSpeed()); // using speed field to store car counts as before

                int carCounts = segState.getCarCounts();
                int averageSpeed = segState.getAverageSpeed();

                int price = carCounts * 5 + (100 - averageSpeed);
                if (segState.getLastAccident() != null && segState.getLastAccident() >= time - 300) {
                    price /= 2; // Discount if recent accident
                }
                System.out.println("Toll Notification: car " + carId + " enter seg " + seg + " price " + price);

                cState.setBalance(cState.getBalance() + price);

                if (payloadFlag) {
                    cState.setExtraLoad(payload);
                }

                // Account Balance Check (inputRecord.getType() == 2)
                if (inputRecord.getType() == 2) {
                    int balance = cState.getBalance();
                    System.out.println("Account Balance: car " + carId + " balance " + balance);
                    System.out.println("GT: " + inputRecord.getSegID() + "-" + carId + ", " + currentTime + ", " + latency + ", " + inputRecord.getTupleNumber());
                } else {
                    // Daily expense calculation
                    if (cState.getLastDay() != null && time - cState.getLastDay() >= 86400) {
                        cState.setLastDay(time);
                        cState.setDailyExpense(0);
                    }

                    // Using speed field as cost component (same logic as before)
                    cState.setDailyExpense(cState.getDailyExpense() + inputRecord.getSpeed());

                    if (payloadFlag) {
                        cState.setExtraLoad(payload);
                    }

                    System.out.println("GT: " + inputRecord.getSegID() + "-" + carId + ", " + currentTime + ", " + latency + ", " + inputRecord.getTupleNumber());

                    // Daily Expense Check (inputRecord.getType() == 3)
                    if (inputRecord.getType() == 3) {
                        int expense = cState.getDailyExpense();
                        System.out.println("Daily Expense: car " + carId + " expense " + expense);
                    }
                }
            }

            // Delay to simulate processing
            DelayUtil.delay(averageDelay);

            // Store updated states back to map state
            segmentStateMap.put(seg, segState);
            carStateMap.put(carId, cState);

            // Emit updated record with TollNotification_Output
            out.collect(new Tuple2<>(carId ,new LinearRoadRecord(
                    inputRecord.getSegID(),
                    inputRecord.getType(),
                    carId,
                    inputRecord.getSpeed(),
                    inputRecord.getXway(),
                    inputRecord.getLane(),
                    inputRecord.getDir(),
                    seg,
                    inputRecord.getPos(),
                    inputRecord.getTime(),
                    inputRecord.getQueryID(),
                    inputRecord.getQStart(),
                    inputRecord.getQEnd(),
                    inputRecord.getQDayOfWeek(),
                    inputRecord.getQMinutes(),
                    inputRecord.getQDay(),
                    TollNotification_Output,
                    inputRecord.getArrivalTime(),
                    inputRecord.getTupleNumber())
            ));
        }

        @Override
        public void open(Configuration config) {
            MapStateDescriptor<Integer, SegmentState> segmentDesc = new MapStateDescriptor<>(
                    "segment-state",
                    Integer.class,
                    SegmentState.class
            );
            segmentStateMap = getRuntimeContext().getMapState(segmentDesc);

            MapStateDescriptor<String, CarState> carDesc = new MapStateDescriptor<>(
                    "car-state",
                    String.class,
                    CarState.class
            );
            carStateMap = getRuntimeContext().getMapState(carDesc);
        }
    }

}
