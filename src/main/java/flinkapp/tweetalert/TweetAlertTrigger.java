package flinkapp.tweetalert;

import Nexmark.sources.Util;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple9;
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
import java.util.*;

public class TweetAlertTrigger {
    private static final int Source_Output = 0;
    private static final int SentimentAnalysis_Output = 1;
    private static final int InfluenceScoring_Output = 2;
    private static final int ContentCategorization_Output = 3;
    private static final int InfluenceScoringAndContentCategorization_Output = 2;
    private static final int Join_Output = 4;
    private static final int Aggregation_Output = 5;
    private static final int AlertTrigger_Output = 6;

    public static void main(String[] args) throws Exception {
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        env.setStateBackend(new MemoryStateBackend(1073741824));
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        DataStreamSource<Tuple7<String, String, String, Integer, Integer, Long, Long>> source =
                env.addSource(new TweetSource(params.get("file_name", "/home/samza/Tweet_data/3hr.txt"),
                                params.getLong("warmup_time", 30L) * 1000,
                                params.getLong("warmup_rate", 1500L),
                                params.getLong("skip_interval", 0L) * 20))
                        .setParallelism(params.getInt("p1", 1));

        DataStream<Tuple10<String, String, String, Integer, Integer, Integer, Double, String, Long, Long>> afterSentimentAnalysis = source
                .keyBy(TweetSource.Tweet_ID)
                .flatMap(new SentimentAnalysis(params.getInt("op2Delay", 1000)))
                .disableChaining()
                .name("Sentiment Analysis")
                .uid("op2")
                .setParallelism(params.getInt("p2", 1))
                .setMaxParallelism(params.getInt("mp2", 8))
                .slotSharingGroup("g2");

//        DataStream<Tuple10<String, String, String, Integer, Integer, Integer, Double, String, Long, Long>> afterInfluenceScoring = source
//                .keyBy(TweetSource.Tweet_ID)
//                .flatMap(new InfluenceScoring(params.getInt("op3Delay", 1000)))
//                .disableChaining()
//                .name("Influence Scoring")
//                .uid("op3")
//                .setParallelism(params.getInt("p3", 1))
//                .setMaxParallelism(params.getInt("mp3", 8))
//                .slotSharingGroup("g3");
//        DataStream<Tuple10<String, String, String, Integer, Integer, Integer, Double, String, Long, Long>> afterContentCategorization = source
//                .keyBy(TweetSource.Tweet_ID)
//                .flatMap(new ContentCategorization(params.getInt("op4Delay", 1000)))
//                .disableChaining()
//                .name("Content Categorization")
//                .uid("op4")
//                .setParallelism(params.getInt("p4", 1))
//                .setMaxParallelism(params.getInt("mp4", 8))
//                .slotSharingGroup("g4");
        DataStream<Tuple10<String, String, String, Integer, Integer, Integer, Double, String, Long, Long>> afterInfluenceScoring = source
                .keyBy(TweetSource.Tweet_ID)
                .flatMap(new InfluenceScoringAndContentCategorization(params.getInt("op3Delay", 1000)))
                .disableChaining()
                .name("Influence Scoring And Content Categorization")
                .uid("op3")
                .setParallelism(params.getInt("p3", 1))
                .setMaxParallelism(params.getInt("mp3", 8))
                .slotSharingGroup("g3");
        DataStream<Tuple10<String, String, String, Integer, Integer, Double, Double, String, Long, Long>> afterJoin = afterSentimentAnalysis.union(afterInfluenceScoring)
                .keyBy(TweetSource.Tweet_ID)
                .flatMap(new TweetJoin(params.getInt("op4Delay", 1000)))
                .disableChaining()
                .name("Join")
                .uid("op4")
                .setParallelism(params.getInt("p4", 1))
                .setMaxParallelism(params.getInt("mp4", 8))
                .slotSharingGroup("g4");
        afterJoin
                .keyBy(TweetSource.Tweet_ID)
                .flatMap(new TweetAggregateAndAlertTrigger(params.getInt("op5Delay", 1000)))
                .disableChaining()
                .name("Aggregate")
                .uid("op5")
                .setParallelism(params.getInt("p5", 1))
                .setMaxParallelism(params.getInt("mp5", 8))
                .slotSharingGroup("g5");

//        DataStream<Tuple10<String, String, String, Integer, Integer, Double, Double, String, Long, Long>> afterAggregate = afterJoin
//                .keyBy(TweetSource.Tweet_ID)
//                .flatMap(new TweetAggregate(params.getInt("op5Delay", 1000)))
//                .disableChaining()
//                .name("Aggregate")
//                .uid("op5")
//                .setParallelism(params.getInt("p5", 1))
//                .setMaxParallelism(params.getInt("mp5", 8))
//                .slotSharingGroup("g5");
//        afterAggregate
//                .keyBy(TweetSource.Tweet_ID)
//                .flatMap(new AlertTrigger(params.getInt("op6Delay", 1000)))
//                .disableChaining()
//                .name("Alert Trigger")
//                .uid("op6")
//                .setParallelism(params.getInt("p6", 1))
//                .setMaxParallelism(params.getInt("mp6", 8))
//                .slotSharingGroup("g6");
        env.execute();
    }
    public static final class TweetSource extends RichParallelSourceFunction<Tuple7<String, String, String, Integer, Integer, Long, Long>> {
        private volatile boolean running = true;
        private static final int Tweet_ID = 0;
        private static final int User_ID = 1;
        private static final int Content = 2;
        private static final int Timestamp = 3;
        private static final int Follower_Count = 4;
        private static final int Arrival_Time = 5;
        private static final int Tuple_Number = 6;

        private final String FILE;
        private final long warmup, warmp_rate, skipCount;

        public static String getTweetID(int tweet_ID){
            return "Tweet_" + tweet_ID;
        }


        public TweetSource(String FILE, long warmup, long warmup_rate, long skipCount) {
            this.FILE = FILE;
            this.warmup = warmup;
            this.warmp_rate = warmup_rate;
            this.skipCount = skipCount;
        }

        @Override
        public void run(SourceContext<Tuple7<String, String, String, Integer, Integer, Long, Long>> ctx) throws Exception {
            String sCurrentLine;
            List<String> textList = new ArrayList<>();
            FileReader stream = null;
            // // for loop to generate message
            BufferedReader br = null;
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
                    String tweet_id = getTweetID(i % 1000000);
                    String user_id = getTweetID(i % 10000);
                    ctx.collect(Tuple7.of(tweet_id, user_id, "test test", 0, 0, System.currentTimeMillis(), (long) count));
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
                                String tweet_id = getTweetID(i % 1000000);
                                String user_id = getTweetID(i % 10000);
                                ctx.collect(Tuple7.of(tweet_id, user_id, "test test", 0, 0, System.currentTimeMillis(), (long) count));
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

                    if (sCurrentLine.split(",").length < 5) {
                        continue;
                    }

                    if (sleepCnt > skipCount) {
                        Long ts = System.currentTimeMillis();
                        String msg = sCurrentLine;
                        List<String> stockArr = Arrays.asList(msg.split(","));
                        ctx.collect(new Tuple7<>(
                                stockArr.get(0),
                                stockArr.get(1),
                                stockArr.get(2),
                                Integer.parseInt(stockArr.get(3)),
                                Integer.parseInt(stockArr.get(4)),
                                ts, (long) count));
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

    public static final class SentimentAnalysis extends RichFlatMapFunction<
            Tuple7<String, String, String, Integer, Integer, Long, Long>,
            Tuple10<String, String, String, Integer, Integer, Integer, Double, String, Long, Long>> {

        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private int averageDelay; // Microsecond
        private Map<String, Double> sentimentDict;
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

        private double getSentiment(String text){
            // TODO: replace with NLP model
            double sentiment = 0;
            int n = 0;
            for(String word: text.split(" ")){
                sentiment += sentimentDict.getOrDefault(word, randomGen.nextUniform(-0.5, 0.5));
                n++;
            }
            if(n==0){
                return 0.0;
            }
            return sentiment/n;
        }
        @Override
        public void flatMap(Tuple7<String, String, String, Integer, Integer, Long, Long> input, Collector<Tuple10<String, String, String, Integer, Integer, Integer, Double, String, Long, Long>> out) throws Exception {
            double sentiment = getSentiment(input.f2);
            delay(averageDelay);
            out.collect(new Tuple10<>(
                    input.f0,
                    input.f1,
                    input.f2,
                    input.f3,
                    input.f4,
                    SentimentAnalysis_Output,
                    sentiment,
                    "",
                    input.f5,
                    input.f6
            ));
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
        }
    }

    public static final class InfluenceScoring extends RichFlatMapFunction<
            Tuple7<String, String, String, Integer, Integer, Long, Long>,
            Tuple10<String, String, String, Integer, Integer, Integer, Double, String, Long, Long>> {

        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private int averageDelay; // Microsecond

        public InfluenceScoring(int _averageDelay) {
            this.averageDelay = _averageDelay;
        }

        private double getInfluenceScore(String userId, int follower_count){
            return Math.log(follower_count);
        }
        @Override
        public void flatMap(Tuple7<String, String, String, Integer, Integer, Long, Long> input, Collector<Tuple10<String, String, String, Integer, Integer, Integer, Double, String, Long, Long>> out) throws Exception {
            double influence = getInfluenceScore(input.f1, input.f4);
            delay(averageDelay);
            out.collect(new Tuple10<>(
                    input.f0,
                    input.f1,
                    input.f2,
                    input.f3,
                    input.f4,
                    InfluenceScoring_Output,
                    influence,
                    "",
                    input.f5,
                    input.f6
            ));
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
        }
    }

    public static final class ContentCategorization extends RichFlatMapFunction<
            Tuple7<String, String, String, Integer, Integer, Long, Long>,
            Tuple10<String, String, String, Integer, Integer, Integer, Double, String, Long, Long>> {

        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private int averageDelay; // Microsecond

        public ContentCategorization(int _averageDelay) {
            this.averageDelay = _averageDelay;
        }

        private String getTopic(String text) {
            // TODO: replace with topic model
            String [] splits = text.split(" ");
            if (splits.length > 0) {
                return splits[0];
            }else{
                return "Empty";
            }
        }

        @Override
        public void flatMap(Tuple7<String, String, String, Integer, Integer, Long, Long> input, Collector<Tuple10<String, String, String, Integer, Integer, Integer, Double, String, Long, Long>> out) throws Exception {
            String topic = getTopic(input.f2);
            delay(averageDelay);
            out.collect(new Tuple10<>(
                    input.f0,
                    input.f1,
                    input.f2,
                    input.f3,
                    input.f4,
                    ContentCategorization_Output,
                    0.0,
                    topic,
                    input.f5,
                    input.f6
            ));
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
        }
    }

    public static final class InfluenceScoringAndContentCategorization extends RichFlatMapFunction<
            Tuple7<String, String, String, Integer, Integer, Long, Long>,
            Tuple10<String, String, String, Integer, Integer, Integer, Double, String, Long, Long>> {

        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private int averageDelay; // Microsecond

        public InfluenceScoringAndContentCategorization(int _averageDelay) {
            this.averageDelay = _averageDelay;
        }

        private double getInfluenceScore(String userId, int follower_count){
            return Math.log(follower_count);
        }

        private String getTopic(String text) {
            // TODO: replace with topic model
            String [] splits = text.split(" ");
            if (splits.length > 0) {
                return splits[0];
            }else{
                return "Empty";
            }
        }

        @Override
        public void flatMap(Tuple7<String, String, String, Integer, Integer, Long, Long> input, Collector<Tuple10<String, String, String, Integer, Integer, Integer, Double, String, Long, Long>> out) throws Exception {
            double influence = getInfluenceScore(input.f1, input.f4);
            String topic = getTopic(input.f2);
            delay(averageDelay);
            out.collect(new Tuple10<>(
                    input.f0,
                    input.f1,
                    input.f2,
                    input.f3,
                    input.f4,
                    InfluenceScoringAndContentCategorization_Output,
                    influence,
                    topic,
                    input.f5,
                    input.f6
            ));
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
        }
    }


    public static final class TweetJoin extends RichFlatMapFunction<
            Tuple10<String, String, String, Integer, Integer, Integer, Double, String, Long, Long>,
            Tuple10<String, String, String, Integer, Integer, Double, Double, String, Long, Long>> {

        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private int averageDelay; // Microsecond

        private transient MapState<String, Double> tweetSentiment, tweetInfluence;
        private transient MapState<String, String> tweetTopic;

        public TweetJoin(int _averageDelay) {
            this.averageDelay = _averageDelay;
        }

        @Override
        public void flatMap(Tuple10<String, String, String, Integer, Integer, Integer, Double, String, Long, Long> input, Collector<Tuple10<String, String, String, Integer, Integer, Double, Double, String, Long, Long>> out) throws Exception {
            String tweetId = input.f0;
            int type = input.f5;
            if(type == SentimentAnalysis_Output){
                double sentiment = input.f6;
                tweetSentiment.put(tweetId, sentiment);
            }else if(type == InfluenceScoringAndContentCategorization_Output){
                double influence = input.f6;
                String topic = input.f7;
                tweetInfluence.put(tweetId, influence);
                tweetTopic.put(tweetId, topic);
            }
//            else if(type == InfluenceScoring_Output){
//                double influence = input.f6;
//                tweetInfluence.put(tweetId, influence);
//            }else if(type == ContentCategorization_Output){
//                String topic = input.f7;
//                tweetTopic.put(tweetId, topic);
//            }
            if(tweetSentiment.contains(tweetId) && tweetTopic.contains(tweetId) && tweetInfluence.contains(tweetId)) {
                double sentiment = tweetSentiment.get(tweetId), influence = tweetInfluence.get(tweetId);
                String topic = tweetTopic.get(tweetId);
                tweetSentiment.remove(tweetId);
                tweetInfluence.remove(tweetId);
                tweetTopic.remove(tweetId);
                out.collect(new Tuple10<>(
                        input.f0,
                        input.f1,
                        input.f2,
                        input.f3,
                        input.f4,
                        sentiment,
                        influence,
                        topic,
                        input.f8,
                        input.f9
                ));
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
            MapStateDescriptor<String, Double> descriptor =
                    new MapStateDescriptor<>("join-sentiment", String.class, Double.class);
            tweetSentiment = getRuntimeContext().getMapState(descriptor);
            descriptor = new MapStateDescriptor<>("join-influence", String.class, Double.class);
            tweetInfluence = getRuntimeContext().getMapState(descriptor);
            MapStateDescriptor<String, String> descriptor1 =
                    new MapStateDescriptor<>("join-topic", String.class, String.class);
            tweetTopic = getRuntimeContext().getMapState(descriptor1);
        }
    }

    public static final class TweetAggregate extends RichFlatMapFunction<
            Tuple10<String, String, String, Integer, Integer, Double, Double, String, Long, Long>,
            Tuple10<String, String, String, Integer, Integer, Double, Double, String, Long, Long>> {

        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private int averageDelay; // Microsecond

        private transient MapState<String, Double> topicTotalSentiment, topicTotalInfluence;

        public TweetAggregate(int _averageDelay) {
            this.averageDelay = _averageDelay;
        }

        @Override
        public void flatMap(Tuple10<String, String, String, Integer, Integer, Double, Double, String, Long, Long> input, Collector<Tuple10<String, String, String, Integer, Integer, Double, Double, String, Long, Long>> out) throws Exception {
            String tweetId = input.f0;
            String topic = input.f7;
            double old_sentiment = 0.0, old_influence = 0.0;
            if(topicTotalSentiment.contains(topic)){
                old_sentiment = topicTotalSentiment.get(topic);
            }
            if(topicTotalInfluence.contains(topic)){
                old_influence = topicTotalInfluence.get(topic);
            }
            double sentiment = old_sentiment + input.f5, influence = old_influence + input.f6;
            topicTotalSentiment.put(topic, sentiment);
            topicTotalInfluence.put(topic, influence);
            out.collect(new Tuple10<>(
                    input.f0,
                    input.f1,
                    input.f2,
                    input.f3,
                    input.f4,
                    sentiment,
                    influence,
                    topic,
                    input.f8,
                    input.f9
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
            MapStateDescriptor<String, Double> descriptor =
                    new MapStateDescriptor<>("aggregate-sentiment", String.class, Double.class);
            topicTotalSentiment = getRuntimeContext().getMapState(descriptor);
            descriptor = new MapStateDescriptor<>("aggregate-influence", String.class, Double.class);
            topicTotalInfluence = getRuntimeContext().getMapState(descriptor);
        }
    }

    public static final class AlertTrigger extends RichFlatMapFunction<
            Tuple10<String, String, String, Integer, Integer, Double, Double, String, Long, Long>,
            Tuple10<String, String, String, Integer, Integer, Double, Double, String, Long, Long>> {

        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private int averageDelay; // Microsecond

        public AlertTrigger(int _averageDelay) {
            this.averageDelay = _averageDelay;
        }

        @Override
        public void flatMap(Tuple10<String, String, String, Integer, Integer, Double, Double, String, Long, Long> input, Collector<Tuple10<String, String, String, Integer, Integer, Double, Double, String, Long, Long>> out) throws Exception {
            delay(averageDelay);
            String tweetId = input.f0, topic = input.f7;
            double sentiment = input.f5, influence = input.f6;
            if(Math.abs(sentiment) > 10.0 && influence >= 10.0){
                System.out.println("Topic Alert: " + topic + " sentiment=" + sentiment + " influence=" + influence);
            }
            long currentTime = System.currentTimeMillis();
            System.out.println("GT: " + input.f0 + ", " + currentTime + ", " + (currentTime - input.f8) + ", " + input.f9);
            out.collect(new Tuple10<>(
                    input.f0,
                    input.f1,
                    input.f2,
                    input.f3,
                    input.f4,
                    input.f5,
                    input.f6,
                    input.f7,
                    input.f8,
                    input.f9
            ));

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
        }
    }

    public static final class TweetAggregateAndAlertTrigger extends RichFlatMapFunction<
            Tuple10<String, String, String, Integer, Integer, Double, Double, String, Long, Long>,
            Tuple10<String, String, String, Integer, Integer, Double, Double, String, Long, Long>> {

        private RandomDataGenerator randomGen = new RandomDataGenerator();
        private int averageDelay; // Microsecond

        private transient MapState<String, Double> topicTotalSentiment, topicTotalInfluence;

        public TweetAggregateAndAlertTrigger(int _averageDelay) {
            this.averageDelay = _averageDelay;
        }

        @Override
        public void flatMap(Tuple10<String, String, String, Integer, Integer, Double, Double, String, Long, Long> input, Collector<Tuple10<String, String, String, Integer, Integer, Double, Double, String, Long, Long>> out) throws Exception {
            String tweetId = input.f0;
            String topic = input.f7;
            double old_sentiment = 0.0, old_influence = 0.0;
            if(topicTotalSentiment.contains(topic)){
                old_sentiment = topicTotalSentiment.get(topic);
            }
            if(topicTotalInfluence.contains(topic)){
                old_influence = topicTotalInfluence.get(topic);
            }
            double sentiment = old_sentiment + input.f5, influence = old_influence + input.f6;
            topicTotalSentiment.put(topic, sentiment);
            topicTotalInfluence.put(topic, influence);
            delay(averageDelay);

            if(Math.abs(sentiment) > 10.0 && influence >= 10.0){
                System.out.println("Topic Alert: " + topic + " sentiment=" + sentiment + " influence=" + influence);
            }
            long currentTime = System.currentTimeMillis();
            System.out.println("GT: " + input.f0 + ", " + currentTime + ", " + (currentTime - input.f8) + ", " + input.f9);
            out.collect(new Tuple10<>(
                    input.f0,
                    input.f1,
                    input.f2,
                    input.f3,
                    input.f4,
                    input.f5,
                    input.f6,
                    input.f7,
                    input.f8,
                    input.f9
            ));

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
            MapStateDescriptor<String, Double> descriptor =
                    new MapStateDescriptor<>("aggregate-alert-sentiment", String.class, Double.class);
            topicTotalSentiment = getRuntimeContext().getMapState(descriptor);
            descriptor = new MapStateDescriptor<>("aggregate-alert-influence", String.class, Double.class);
            topicTotalInfluence = getRuntimeContext().getMapState(descriptor);
        }
    }

}

