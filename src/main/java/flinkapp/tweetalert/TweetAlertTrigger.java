package flinkapp.tweetalert;

import Nexmark.sources.Util;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
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
import java.io.Serializable;
import java.util.*;
import org.apache.flink.api.common.state.StateTtlConfig;

public class TweetAlertTrigger {
    private static final int Source_Output = 0;
    private static final int SentimentAnalysis_Output = 1;
    private static final int InfluenceScoring_Output = 2;
    private static final int ContentCategorization_Output = 3;
    private static final int InfluenceScoringAndContentCategorization_Output = 2;
    private static final int Join_Output = 4;
    private static final int Aggregation_Output = 5;
    private static final int AlertTrigger_Output = 6;

    public static class TweetRecord {
        private String tweetId;
        private String userId;
        private String content;
        private int timestamp;
        private int followerCount;
        private long arrivalTime;
        private long tupleNumber;

        // Default constructor
        public TweetRecord() {}

        public TweetRecord(String tweetId, String userId, String content, int timestamp, int followerCount, long arrivalTime, long tupleNumber) {
            this.tweetId = tweetId;
            this.userId = userId;
            this.content = content;
            this.timestamp = timestamp;
            this.followerCount = followerCount;
            this.arrivalTime = arrivalTime;
            this.tupleNumber = tupleNumber;
        }

        // Getters and Setters
        public String getTweetId() { return tweetId; }
        public void setTweetId(String tweetId) { this.tweetId = tweetId; }

        public String getUserId() { return userId; }
        public void setUserId(String userId) { this.userId = userId; }

        public String getContent() { return content; }
        public void setContent(String content) { this.content = content; }

        public int getTimestamp() { return timestamp; }
        public void setTimestamp(int timestamp) { this.timestamp = timestamp; }

        public int getFollowerCount() { return followerCount; }
        public void setFollowerCount(int followerCount) { this.followerCount = followerCount; }

        public long getArrivalTime() { return arrivalTime; }
        public void setArrivalTime(long arrivalTime) { this.arrivalTime = arrivalTime; }

        public long getTupleNumber() { return tupleNumber; }
        public void setTupleNumber(long tupleNumber) { this.tupleNumber = tupleNumber; }
    }

    public static class TweetResult {
        private String tweetId;
        private String userId;
        private String content;
        private int timestamp;
        private int followerCount;
        private int operatorType;
        private double result_value;
        private String topic;
        private long arrivalTime;
        private long tupleNumber;

        // Default constructor
        public TweetResult() {}

        public TweetResult(String tweetId, String userId, String content, int timestamp, int followerCount,
                           int operatorType, double result_value, String topic, long arrivalTime, long tupleNumber) {
            this.tweetId = tweetId;
            this.userId = userId;
            this.content = content;
            this.timestamp = timestamp;
            this.followerCount = followerCount;
            this.operatorType = operatorType;
            this.result_value = result_value;
            this.topic = topic;
            this.arrivalTime = arrivalTime;
            this.tupleNumber = tupleNumber;
        }

        // Getters and Setters
        public String getTweetId() { return tweetId; }
        public void setTweetId(String tweetId) { this.tweetId = tweetId; }

        public String getUserId() { return userId; }
        public void setUserId(String userId) { this.userId = userId; }

        public String getContent() { return content; }
        public void setContent(String content) { this.content = content; }

        public int getTimestamp() { return timestamp; }
        public void setTimestamp(int timestamp) { this.timestamp = timestamp; }

        public int getFollowerCount() { return followerCount; }
        public void setFollowerCount(int followerCount) { this.followerCount = followerCount; }

        public int getOperatorType() { return operatorType; }
        public void setOperatorType(int operatorType) { this.operatorType = operatorType; }

        public double getResult_value() { return result_value; }
        public void setResult_value(double result_value) { this.result_value = result_value; }

        public String getTopic() { return topic; }
        public void setTopic(String topic) { this.topic = topic; }

        public long getArrivalTime() { return arrivalTime; }
        public void setArrivalTime(long arrivalTime) { this.arrivalTime = arrivalTime; }

        public long getTupleNumber() { return tupleNumber; }
        public void setTupleNumber(long tupleNumber) { this.tupleNumber = tupleNumber; }
    }

    public static class JoinedResult {
        private String tweetId;
        private String userId;
        private String content;
        private int timestamp;
        private int followerCount;
        private double sentiment;
        private double influence;
        private String topic;
        private long arrivalTime;
        private long tupleNumber;

        // Default constructor
        public JoinedResult () {}

        public JoinedResult(String tweetId, String userId, String content, int timestamp, int followerCount, double sentiment, double influence, String topic, long arrivalTime, long tupleNumber) {
            this.tweetId = tweetId;
            this.userId = userId;
            this.content = content;
            this.timestamp = timestamp;
            this.followerCount = followerCount;
            this.sentiment = sentiment;
            this.influence = influence;
            this.topic = topic;
            this.arrivalTime = arrivalTime;
            this.tupleNumber = tupleNumber;
        }

        // Getters and Setters
        public String getTweetId() { return tweetId; }
        public void setTweetId(String tweetId) { this.tweetId = tweetId; }

        public String getUserId() { return userId; }
        public void setUserId(String userId) { this.userId = userId; }

        public String getContent() { return content; }
        public void setContent(String content) { this.content = content; }

        public int getTimestamp() { return timestamp; }
        public void setTimestamp(int timestamp) { this.timestamp = timestamp; }

        public int getFollowerCount() { return followerCount; }
        public void setFollowerCount(int followerCount) { this.followerCount = followerCount; }
        public double getSentiment() { return sentiment; }
        public void setSentiment(double sentiment) { this.sentiment = sentiment; }
        public double getInfluence() { return influence; }
        public void setInfluence(double influence) { this.influence = influence; }

        public String getTopic() { return topic; }
        public void setTopic(String topic) { this.topic = topic; }

        public long getArrivalTime() { return arrivalTime; }
        public void setArrivalTime(long arrivalTime) { this.arrivalTime = arrivalTime; }

        public long getTupleNumber() { return tupleNumber; }
        public void setTupleNumber(long tupleNumber) { this.tupleNumber = tupleNumber; }
    }

    public static void main(String[] args) throws Exception {
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        env.setStateBackend(new MemoryStateBackend(1073741824));
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        DataStreamSource<Tuple2<String, TweetRecord>> source =
                env.addSource(new TweetSource(params.get("file_name", "/home/samza/Tweet_data/3hr.txt"),
                                params.getLong("warmup_time", 30L) * 1000,
                                params.getLong("warmup_rate", 1500L),
                                params.getLong("skip_interval", 0L) * 20))
                        .setParallelism(params.getInt("p1", 1));

        DataStream<Tuple2<String, TweetResult>> afterSentimentAnalysis = source
                .keyBy(0)
                .flatMap(new SentimentAnalysis(params.getInt("op2Delay", 1000)))
                .disableChaining()
                .name("Sentiment Analysis")
                .uid("op2")
                .setParallelism(params.getInt("p2", 1))
                .setMaxParallelism(params.getInt("mp2", 8))
                .slotSharingGroup("g2");

        DataStream<Tuple2<String, TweetResult>> afterInfluenceScoring = source
                .keyBy(0)
                .flatMap(new InfluenceScoringAndContentCategorization(params.getInt("op3Delay", 1000)))
                .disableChaining()
                .name("Influence Scoring And Content Categorization")
                .uid("op3")
                .setParallelism(params.getInt("p3", 1))
                .setMaxParallelism(params.getInt("mp3", 8))
                .slotSharingGroup("g3");

        DataStream<Tuple2<String, JoinedResult>> afterJoin = afterSentimentAnalysis.union(afterInfluenceScoring)
                .keyBy(0)
                .flatMap(new TweetJoin(params.getInt("op4Delay", 1000)))
                .disableChaining()
                .name("Join")
                .uid("op4")
                .setParallelism(params.getInt("p4", 1))
                .setMaxParallelism(params.getInt("mp4", 8))
                .slotSharingGroup("g4");
        afterJoin
                .keyBy(0)
                .map(new TweetAggregateAndAlertTrigger(params.getInt("op5Delay", 1000)))
                .disableChaining()
                .name("Aggregate")
                .uid("op5")
                .setParallelism(params.getInt("p5", 1))
                .setMaxParallelism(params.getInt("mp5", 8))
                .slotSharingGroup("g5");

        env.execute();
    }
    public static final class TweetSource extends RichParallelSourceFunction<Tuple2<String, TweetRecord>> {
        private volatile boolean running = true;

        private final String FILE;
        private final long warmup, warmp_rate, skipCount;

        public static String getTweetID(int tweet_ID) {
            return "Tweet_" + tweet_ID;
        }

        public TweetSource(String FILE, long warmup, long warmup_rate, long skipCount) {
            this.FILE = FILE;
            this.warmup = warmup;
            this.warmp_rate = warmup_rate;
            this.skipCount = skipCount;
        }

        @Override
        public void run(SourceContext<Tuple2<String, TweetRecord>> ctx) throws Exception {
            String sCurrentLine;
            FileReader stream = null;
            BufferedReader br = null;
            int counter = 0, count = 0;
            int noRecSleepCnt = 0;
            int sleepCnt = 0;

            long startTime = System.currentTimeMillis();
            System.out.println("Warmup start at: " + startTime);

            // Warm-up phase
            while (System.currentTimeMillis() - startTime < warmup) {
                long emitStartTime = System.currentTimeMillis();
                for (int i = 0; i < warmp_rate / 20; i++) {
                    String tweet_id = getTweetID(count % 1000000);
                    String user_id = getTweetID(count % 10000);
                    ctx.collect(new Tuple2<>(user_id, new TweetRecord(
                            tweet_id,
                            user_id,
                            "test test",
                            0,
                            0,
                            System.currentTimeMillis(),
                            count
                    )));
                    count++;
                }
                Util.pause(emitStartTime);
            }

            try {
                stream = new FileReader(FILE);
                br = new BufferedReader(stream);

                long start = System.currentTimeMillis();
                long cur;

                while ((sCurrentLine = br.readLine()) != null && running) {
                    if (sCurrentLine.equals("END")) {
                        sleepCnt++;
                        if (counter == 0) {
                            noRecSleepCnt++;
                            System.out.println("no record in this sleep !" + noRecSleepCnt);
                        }
                        if (sleepCnt <= skipCount) {
                            for (int i = 0; i < warmp_rate / 20; i++) {
                                String tweet_id = getTweetID(count % 1000000);
                                String user_id = getTweetID(count % 10000);
                                ctx.collect(new Tuple2<>(user_id, new TweetRecord(
                                        tweet_id,
                                        user_id,
                                        "test test",
                                        0,
                                        0,
                                        System.currentTimeMillis(),
                                        count
                                )));
                                count++;
                            }
                        }
                        counter = 0;
                        cur = System.currentTimeMillis();
                        if (cur < sleepCnt * 50 + start) {
                            Thread.sleep((sleepCnt * 50 + start) - cur);
                        } else {
                            System.out.println("rate exceeds 50ms.");
                        }
                        continue;
                    }

                    String[] fields = sCurrentLine.split(",");
                    if (fields.length < 5) {
                        continue;
                    }

                    if (sleepCnt > skipCount) {
                        long ts = System.currentTimeMillis();
                        String tweet_id = fields[0];
                        String user_id = getTweetID(count % 10000); // fields[1];
                        String content = fields[2];
                        int timestamp = Integer.parseInt(fields[3]);
                        int followerCount = Integer.parseInt(fields[4]);

                        ctx.collect(new Tuple2<>(user_id, new TweetRecord(
                                tweet_id,
                                user_id,
                                content,
                                timestamp,
                                followerCount,
                                ts,
                                count
                        )));
                        count++;
                    }
                    counter++;
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (stream != null) stream.close();
                if (br != null) br.close();
                ctx.close();
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    public static class DelayUtil {
        private static final RandomDataGenerator randomGen = new RandomDataGenerator();

        public static void delay(long intervalMicroseconds) {
            double ranN = randomGen.nextGaussian(intervalMicroseconds, 1) * 1000;
            long delayNanos = (int) ranN;
            if (delayNanos < 0) delayNanos = intervalMicroseconds * 1000;
            long start = System.nanoTime();
            while (System.nanoTime() - start < delayNanos) {
                // Busy waiting
            }
        }
    }


    public static final class SentimentAnalysis extends RichFlatMapFunction<Tuple2<String, TweetRecord>, Tuple2<String, TweetResult>> {

        private final RandomDataGenerator randomGen = new RandomDataGenerator();
        private final int averageDelay; // in microseconds
        private final Map<String, Double> sentimentDict;

        public SentimentAnalysis(int _averageDelay) {
            this.averageDelay = _averageDelay;
            sentimentDict = new HashMap<>();
            sentimentDict.put("good", 1.0);
            sentimentDict.put("excellent", 1.0);
            sentimentDict.put("well", 1.0);
            sentimentDict.put("wonderful", 1.0);
            sentimentDict.put("nice", 1.0);
            sentimentDict.put("bad", -1.0);
            sentimentDict.put("terrible", -1.0);
            sentimentDict.put("awful", -1.0);
            sentimentDict.put("worse", -1.0);
            sentimentDict.put("ugly", -1.0);
        }

        private double getSentiment(String text) {
            // TODO: Replace with an actual NLP model
            double sentiment = 0;
            int n = 0;
            for (String word : text.split(" ")) {
                sentiment += sentimentDict.getOrDefault(word, randomGen.nextUniform(-0.5, 0.5));
                n++;
            }
            return (n == 0) ? 0.0 : sentiment / n;
        }

        @Override
        public void flatMap(Tuple2<String, TweetRecord> rawInput, Collector<Tuple2<String, TweetResult>> out) throws Exception {
            TweetRecord input = rawInput.f1;
            double sentiment = getSentiment(input.getContent());
            DelayUtil.delay(averageDelay);

            TweetResult result = new TweetResult(
                    input.getTweetId(),
                    input.getUserId(),
                    input.getContent(),
                    input.getTimestamp(),
                    input.getFollowerCount(),
                    SentimentAnalysis_Output, // constant defined elsewhere
                    sentiment,
                    "", // no topic assigned here
                    input.getArrivalTime(),
                    input.getTupleNumber()
            );

            out.collect(new Tuple2<>(rawInput.f0, result));
        }

        @Override
        public void open(Configuration config) {
            // Optional: Add initialization logic if needed
        }
    }


    public static final class InfluenceScoringAndContentCategorization extends RichFlatMapFunction<
            Tuple2<String, TweetRecord>,
            Tuple2<String, TweetResult>> {

        private final RandomDataGenerator randomGen = new RandomDataGenerator();
        private final int averageDelay; // in microseconds

        public InfluenceScoringAndContentCategorization(int _averageDelay) {
            this.averageDelay = _averageDelay;
        }

        private double getInfluenceScore(String userId, int followerCount) {
            // Simple influence score model, can be replaced with a more sophisticated model
            return Math.log(followerCount);
        }

        private String getTopic(String text) {
            // TODO: replace with a more advanced topic model
            String[] splits = text.split(" ");
            if (splits.length > 0) {
                return splits[0].length() > 5 ? splits[0].substring(0, 5) : splits[0];
            } else {
                return "Empty";
            }
        }

        @Override
        public void flatMap(Tuple2<String, TweetRecord> rawInput, Collector<Tuple2<String, TweetResult>> out) throws Exception {
            TweetRecord input = rawInput.f1;
            double influence = getInfluenceScore(input.getUserId(), input.getFollowerCount());
            String topic = getTopic(input.getContent());

            DelayUtil.delay(averageDelay); // Use the same delay utility as in SentimentAnalysis

            TweetResult result = new TweetResult(
                    input.getTweetId(),
                    input.getUserId(),
                    input.getContent(),
                    input.getTimestamp(),
                    input.getFollowerCount(),
                    InfluenceScoringAndContentCategorization_Output,  // previously a constant
                    influence,
                    topic,
                    input.getArrivalTime(),
                    input.getTupleNumber()
            );

            // Output is (key, result) where key = tweetId
            out.collect(new Tuple2<>(rawInput.f0, result));
        }

        @Override
        public void open(Configuration config) {
            // Add initialization logic if needed
        }
    }


    // Define the TweetJoin class
    public static final class TweetJoin extends RichFlatMapFunction<
            Tuple2<String, TweetResult>,
            Tuple2<String, JoinedResult>> {

        private static final long serialVersionUID = 1L;
        public static class TweetMetrics implements Serializable {
            private double sentiment;
            private double influence;
            private String topic;

            public TweetMetrics() {
                this.sentiment = 0.0;
                this.influence = -1.0;
                this.topic = null;
            }

            public double getSentiment() {
                return sentiment;
            }

            public void setSentiment(double sentiment) {
                this.sentiment = sentiment;
            }

            public double getInfluence() {
                return influence;
            }

            public void setInfluence(double influence) {
                this.influence = influence;
            }

            public String getTopic() {
                return topic;
            }

            public void setTopic(String topic) {
                this.topic = topic;
            }

            // Utility method to check if both sentiment and influence are set
            public boolean isComplete() {
                return this.influence != -1.0 && this.topic != null;
            }
        }

        private final int averageDelay; // in microseconds

        // Single MapState to hold TweetMetrics per tweetId
        private transient MapState<String, TweetMetrics> tweetMetricsState;

        // TTL duration (e.g., 10 minutes)
        private static final Time STATE_TTL = Time.minutes(5);

        public TweetJoin(int _averageDelay) {
            this.averageDelay = _averageDelay;
        }

        @Override
        public void flatMap(Tuple2<String, TweetResult> inputTuple, Collector<Tuple2<String, JoinedResult>> out) throws Exception {
            String tweetId = inputTuple.f0;
            TweetResult input = inputTuple.f1;
            int type = input.getOperatorType();

            // Retrieve existing metrics or initialize if not present
            TweetMetrics metrics = tweetMetricsState.get(tweetId);
            if (metrics == null) {
                metrics = new TweetMetrics();
            }

            // Update the metrics based on operator type
            if (type == SentimentAnalysis_Output) {
                double sentiment = input.getResult_value();
                metrics.setSentiment(sentiment);
            } else if (type == InfluenceScoringAndContentCategorization_Output) {
                double influence = input.getResult_value();
                String topic = input.getTopic();
                metrics.setInfluence(influence);
                metrics.setTopic(topic);
            }

            // Update the state with the new metrics
            tweetMetricsState.put(tweetId, metrics);

            // Simulate processing delay
            DelayUtil.delay(averageDelay);

            // Check if the metrics are complete
            if (metrics.isComplete()) {
                // Construct the final joined result
                JoinedResult joinedResult = new JoinedResult(
                        input.getTweetId(),
                        input.getUserId(),
                        input.getContent(),
                        input.getTimestamp(),
                        input.getFollowerCount(),
                        metrics.getSentiment(),
                        metrics.getInfluence(),
                        metrics.getTopic(),
                        input.getArrivalTime(),
                        input.getTupleNumber()
                );

                // Emit the joined result
                out.collect(new Tuple2<>(tweetId, joinedResult));

                // Remove the entry from the state as the join is complete
                tweetMetricsState.remove(tweetId);
            } else {
                // Emit a partial JoinedResult with default values
                JoinedResult partialResult = new JoinedResult(
                        input.getTweetId(),
                        input.getUserId(),
                        input.getContent(),
                        input.getTimestamp(),
                        input.getFollowerCount(),
                        -1.0, // Default sentiment
                        -1.0, // Default influence
                        input.getTopic(),
                        input.getArrivalTime(),
                        input.getTupleNumber()
                );
                out.collect(new Tuple2<>(tweetId, partialResult));
            }
        }

        @Override
        public void open(Configuration config) {
            // Configure TTL for the state to automatically clean up stale entries
            StateTtlConfig ttlConfig = StateTtlConfig
                    .newBuilder(STATE_TTL)
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                    .build();

            // Define MapStateDescriptor with TTL configuration
            MapStateDescriptor<String, TweetMetrics> descriptor =
                    new MapStateDescriptor<>(
                            "tweet-metrics-state", // State name
                            String.class,          // Key type
                            TweetMetrics.class     // Value type
                    );

            // Apply TTL configuration to the state descriptor
            descriptor.enableTimeToLive(ttlConfig);

            // Initialize the MapState with the descriptor
            tweetMetricsState = getRuntimeContext().getMapState(descriptor);
        }
    }

    public static final class TweetAggregateAndAlertTrigger extends RichMapFunction<
            Tuple2<String, JoinedResult>,
            Tuple2<String, JoinedResult>> {

        public static class TopicMetrics implements Serializable {
            private double totalSentiment;
            private double totalInfluence;

            // Default constructor
            public TopicMetrics() {
                this.totalSentiment = 0.0;
                this.totalInfluence = 0.0;
            }

            // Parameterized constructor
            public TopicMetrics(double totalSentiment, double totalInfluence) {
                this.totalSentiment = totalSentiment;
                this.totalInfluence = totalInfluence;
            }

            // Getters and Setters
            public double getTotalSentiment() {
                return totalSentiment;
            }

            public void setTotalSentiment(double totalSentiment) {
                this.totalSentiment = totalSentiment;
            }

            public double getTotalInfluence() {
                return totalInfluence;
            }

            public void setTotalInfluence(double totalInfluence) {
                this.totalInfluence = totalInfluence;
            }

            // Methods to update metrics
            public void addSentiment(double sentiment) {
                this.totalSentiment += sentiment;
            }

            public void addInfluence(double influence) {
                this.totalInfluence += influence;
            }
        }

        private static final long serialVersionUID = 1L;

        private final int averageDelay; // Microseconds

        // Single MapState to hold both sentiment and influence per topic
        private transient MapState<String, TopicMetrics> topicMetricsState;

        // TTL duration for state entries (e.g., 5 minutes)
        private static final Time STATE_TTL = Time.minutes(5);

        public TweetAggregateAndAlertTrigger(int _averageDelay) {
            this.averageDelay = _averageDelay;
        }

        @Override
        public Tuple2<String, JoinedResult> map(Tuple2<String, JoinedResult> inputTuple) throws Exception {
            String tweetId = inputTuple.f0;
            JoinedResult input = inputTuple.f1;
            String topic = input.getTopic();

            if (input.getSentiment() > -0.99 || input.getInfluence() > -0.99) {

                // Retrieve existing metrics or initialize if not present
                TopicMetrics metrics = topicMetricsState.get(topic);
                if (metrics == null) {
                    metrics = new TopicMetrics();
                }

                // Update the metrics with the current tweet's sentiment and influence
                metrics.addSentiment(input.getSentiment());
                metrics.addInfluence(input.getInfluence());

                // Update the state
                topicMetricsState.put(topic, metrics);

                // Simulate processing delay
                DelayUtil.delay(averageDelay);

                // Check for alert conditions
                if (Math.abs(metrics.getTotalSentiment()) > 10.0 && metrics.getTotalInfluence() >= 10.0) {
                    System.out.println("Topic Alert: " + topic + " sentiment=" + metrics.getTotalSentiment()
                            + " influence=" + metrics.getTotalInfluence());
                }
            } else {
                // Simulate processing delay even if conditions are not met
                DelayUtil.delay(averageDelay);
            }

            // Log the processing information
            long currentTime = System.currentTimeMillis();
            System.out.println("GT: " + tweetId + ", " + currentTime + ", "
                    + (currentTime - input.getArrivalTime()) + ", " + input.getTupleNumber());

            // Emit the same JoinedResult without modification
            return new Tuple2<>(tweetId, input);
        }

        @Override
        public void open(Configuration config) {
            // Configure TTL for the state to automatically clean up stale entries
            StateTtlConfig ttlConfig = StateTtlConfig
                    .newBuilder(STATE_TTL)
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                    .build();

            // Define MapStateDescriptor with TTL configuration
            MapStateDescriptor<String, TopicMetrics> descriptor =
                    new MapStateDescriptor<>(
                            "aggregate-alert-metrics", // State name
                            String.class,               // Key type
                            TopicMetrics.class          // Value type
                    );

            // Apply TTL configuration to the state descriptor
            descriptor.enableTimeToLive(ttlConfig);

            // Initialize the MapState with the descriptor
            topicMetricsState = getRuntimeContext().getMapState(descriptor);
        }
    }
}

