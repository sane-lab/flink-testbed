package flinkapp.MLmodelscoring;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * Fraud-style ML scoring pipeline (mock).
 *
 *  src ─► Parse ▶ FeatureBuild ▶ RealisticGBDTScorer ▶ LatencyTrackingFlatMap ─► output
 *
 *  ▪ Key = userId (String)                              ▪ State backend = RocksDB
 *  ▪ Feature-based processing time simulation           ▪ All parameters overridable via –Dflags
 *  ▪ Ground truth latency tracked and written to console ▪ Similar to LinearRoad pattern
 */
public final class MLScoringJob {

    // Output operator constants following LinearRoad pattern
    private static final int Source_Output = 0;
    private static final int ParseTxn_Output = 1;
    private static final int FeatureBuilder_Output = 2;
    private static final int RealisticGBDTScorer_Output = 3;
    private static final int LatencyTracking_Output = 4;

    public static void main(String[] args) throws Exception {

        // ---------- 1. env & params ----------
        ParameterTool p = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(p);
        env.setStateBackend(new MemoryStateBackend(100000000));
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // env.enableCheckpointing(p.getLong("ckpt", 10_000));

        // parameters - now tunable via command line with robust error handling
        long runSeconds = getLongParameter(p, "run.seconds", 300L);
        
        // Sine curve parameters for arrival rate pattern f(t) = amplitude * sin(t) + baseline
        double sineBaseline = getDoubleParameter(p, "sine.baseline", 1000.0);    // baseline rate (c in f(t)=sin(t)+c)
        double sineAmplitude = getDoubleParameter(p, "sine.amplitude", 300.0);   // amplitude of sine wave
        double sinePeriod = getDoubleParameter(p, "sine.period", 60.0);          // period in seconds
        
        // Warmup parameters similar to LinearRoad
        long warmupTime = getLongParameter(p, "warmup_time", 30L) * 1000; // warmup duration in ms
        long warmupRate = getLongParameter(p, "warmup_rate", 800L);       // warmup rate txn/s (should be <= sineBaseline)
        double inputRateFactor = getDoubleParameter(p, "input_rate_factor", 1.0); // rate multiplier
        
        DataStream<Tuple2<String, MLScoringRecord>> source = env
                .addSource(new TxnSource(
                    runSeconds * 1_000L, 
                    sineBaseline,                            // baseline rate for sine curve
                    sineAmplitude,                           // amplitude of sine wave
                    sinePeriod,                              // sine wave period in seconds
                    getDoubleParameter(p, "spike.probability", 0.05),  // probability of spike per second
                    getDoubleParameter(p, "spike.multiplier", 3.0),    // spike multiplier (3x normal rate)
                    getDoubleParameter(p, "fluctuation.std", 0.1),     // short-term fluctuation std dev (10%)
                    warmupTime,                              // warmup duration
                    warmupRate,                              // warmup rate
                    inputRateFactor                          // input rate factor
                ))
                .name("Mock-Txn-Source")
                .setParallelism(getIntParameter(p, "p1", 1));

        // Parse operator following LinearRoad pattern
        DataStream<Tuple2<String, MLScoringRecord>> afterParse = source
                .keyBy(0)
                .flatMap(new ParseTxn(getLongParameter(p, "parse.delay", 1L)))
                .disableChaining()
                .name("ParseTxn")
                .uid("op2")
                .setParallelism(getIntParameter(p, "p2", 1))
                .setMaxParallelism(getIntParameter(p, "mp2", 8))
                .slotSharingGroup("g2");

        // Feature builder operator
        DataStream<Tuple2<String, MLScoringRecord>> afterFeatureBuilder = afterParse
                .keyBy(0)
                .flatMap(new FeatureBuilder(getLongParameter(p, "feature.delay", 1L)))
                .disableChaining()
                .name("FeatureBuilder")
                .uid("op3")
                .setParallelism(getIntParameter(p, "p3", 1))
                .setMaxParallelism(getIntParameter(p, "mp3", 8))
                .slotSharingGroup("g3");

        // GBDT Scorer operator
        DataStream<Tuple2<String, MLScoringRecord>> afterScorer = afterFeatureBuilder
                .keyBy(0)
                .flatMap(new RealisticGBDTScorer(
                    getLongParameter(p, "scorer.base.delay", 2000L),        // Base processing time (microseconds)
                    getDoubleParameter(p, "scorer.complexity.factor", 1.0) // Complexity multiplier
                ))
                .disableChaining()
                .name("RealisticGBDTScorer")
                .uid("op4")
                .setParallelism(getIntParameter(p, "p4", 1))
                .setMaxParallelism(getIntParameter(p, "mp4", 8))
                .slotSharingGroup("g4");
                
        // Latency tracking flatMap operator (replaces sink)
        DataStream<Tuple2<String, MLScoringRecord>> finalOutput = afterScorer
                .keyBy(0)
                .flatMap(new LatencyTrackingFlatMap())
                .disableChaining()
                .name("LatencyTracking")
                .uid("op5")
                .setParallelism(getIntParameter(p, "p5", 1))
                .setMaxParallelism(getIntParameter(p, "mp5", 8))
                .slotSharingGroup("g5");
                
        env.execute("Real-Time ML Scoring (Mock)");
    }
    
    // Helper methods for robust parameter handling
    private static long getLongParameter(ParameterTool p, String key, long defaultValue) {
        try {
            return p.getLong(key, defaultValue);
        } catch (Exception e) {
            System.out.println("Warning: Could not parse parameter " + key + ", using default: " + defaultValue);
            return defaultValue;
        }
    }
    
    private static int getIntParameter(ParameterTool p, String key, int defaultValue) {
        try {
            return p.getInt(key, defaultValue);
        } catch (Exception e) {
            System.out.println("Warning: Could not parse parameter " + key + ", using default: " + defaultValue);
            return defaultValue;
        }
    }
    
    private static double getDoubleParameter(ParameterTool p, String key, double defaultValue) {
        try {
            return p.getDouble(key, defaultValue);
        } catch (Exception e) {
            System.out.println("Warning: Could not parse parameter " + key + ", using default: " + defaultValue);
            return defaultValue;
        }
    }

    /* ===========================================================
     *  MLScoringRecord - Similar to LinearRoadRecord
     * ===========================================================
     */
    public static class MLScoringRecord {
        private String accountId;
        private Long id;
        private Double amount;
        private Integer merchant;
        private String features;
        private Double score;
        private Integer outputOperator;
        private Long arrivalTime;
        private Long tupleNumber;

        public MLScoringRecord() {}

        public MLScoringRecord(String accountId, Long id, Double amount, Integer merchant, 
                              String features, Double score, Integer outputOperator,
                              Long arrivalTime, Long tupleNumber) {
            this.accountId = accountId;
            this.id = id;
            this.amount = amount;
            this.merchant = merchant;
            this.features = features;
            this.score = score;
            this.outputOperator = outputOperator;
            this.arrivalTime = arrivalTime;
            this.tupleNumber = tupleNumber;
        }

        // Getters
        public String getAccountId() { return accountId; }
        public Long getId() { return id; }
        public Double getAmount() { return amount; }
        public Integer getMerchant() { return merchant; }
        public String getFeatures() { return features; }
        public Double getScore() { return score; }
        public Integer getOutputOperator() { return outputOperator; }
        public Long getArrivalTime() { return arrivalTime; }
        public Long getTupleNumber() { return tupleNumber; }

        // Setters
        public void setAccountId(String accountId) { this.accountId = accountId; }
        public void setId(Long id) { this.id = id; }
        public void setAmount(Double amount) { this.amount = amount; }
        public void setMerchant(Integer merchant) { this.merchant = merchant; }
        public void setFeatures(String features) { this.features = features; }
        public void setScore(Double score) { this.score = score; }
        public void setOutputOperator(Integer outputOperator) { this.outputOperator = outputOperator; }
        public void setArrivalTime(Long arrivalTime) { this.arrivalTime = arrivalTime; }
        public void setTupleNumber(Long tupleNumber) { this.tupleNumber = tupleNumber; }
    }
    
    /* ===========================================================
     *  1 | Synthetic source producing Tuple2<String, MLScoringRecord>
     * ===========================================================
     */
    public static class TxnSource implements SourceFunction<Tuple2<String, MLScoringRecord>> {

        private final long runMillis;
        private final double sineBaseline;
        private final double sineAmplitude;
        private final double sinePeriod;
        private final double spikeProbability;
        private final double spikeMultiplier;
        private final double fluctuationStd;
        private final long warmupTime;
        private final long warmupRate;
        private final double inputRateFactor;
        private volatile boolean running = true;

        public TxnSource(long runMillis, double sineBaseline, double sineAmplitude, double sinePeriod, 
                        double spikeProbability, double spikeMultiplier, double fluctuationStd,
                        long warmupTime, long warmupRate, double inputRateFactor) {
            this.runMillis = runMillis;
            this.sineBaseline = sineBaseline;
            this.sineAmplitude = sineAmplitude;
            this.sinePeriod = sinePeriod;
            this.spikeProbability = spikeProbability;
            this.spikeMultiplier = spikeMultiplier;
            this.fluctuationStd = fluctuationStd;
            this.warmupTime = warmupTime;
            this.warmupRate = warmupRate;
            this.inputRateFactor = inputRateFactor;
        }

        @Override
        public void run(SourceContext<Tuple2<String, MLScoringRecord>> ctx) throws Exception {
            Random rnd = new Random(114514);
            long start = System.currentTimeMillis();
            long id = 0L;

            // Warmup phase - gradually ramp up to sine baseline
            if (warmupTime > 0) {
                System.out.println("Warmup phase for " + warmupTime + "ms, ramping from " + warmupRate + " to " + sineBaseline + " txn/s");
                long warmupStart = System.currentTimeMillis();
                while (System.currentTimeMillis() - warmupStart < warmupTime) {
                    double warmupProgress = (double)(System.currentTimeMillis() - warmupStart) / warmupTime;
                    double currentRate = warmupRate + (sineBaseline - warmupRate) * warmupProgress;
                    
                    long emitStart = System.currentTimeMillis();
                    int recordsToEmit = Math.max(1, (int) Math.round(currentRate * inputRateFactor));
                    for (int i = 0; i < recordsToEmit; i++) {
                        long arrivalTime = System.currentTimeMillis();
                        int accountId = rnd.nextInt(10_000);
                        String accountKey = "ACC" + accountId;
                        
                        MLScoringRecord record = new MLScoringRecord(
                                accountKey,
                                id++,
                                1 + rnd.nextDouble() * 499,    // amount
                                rnd.nextInt(2_000),            // merchant
                                null,                          // features (to be filled later)
                                0.0,                          // score (to be calculated)
                                Source_Output,                 // output operator
                                arrivalTime,                   // arrival time for latency tracking
                                id);                          // tuple number
                        
                        ctx.collect(new Tuple2<>(accountKey, record));
                    }
                    long elapsed = System.currentTimeMillis() - emitStart;
                    if (elapsed < 1_000) Thread.sleep(1_000 - elapsed);
                }
                System.out.println("Warmup phase complete. Starting main run with sine curve pattern.");
            }

            // Main run phase
            double maxRate = sineBaseline + sineAmplitude;
            double minRate = sineBaseline - sineAmplitude;
            System.out.println("Main run phase for " + runMillis + "ms with sine curve: baseline=" + sineBaseline + 
                             ", amplitude=" + sineAmplitude + ", rate range=[" + minRate + "," + maxRate + "] txn/s");
            long mainRunStart = System.currentTimeMillis();
            while (running && System.currentTimeMillis() - mainRunStart < runMillis) {
                long currentTime = System.currentTimeMillis() - mainRunStart;
                double secondsElapsed = currentTime / 1000.0;
                
                // Calculate dynamic rate with sine curve f(t) = amplitude * sin(t) + baseline
                double currentRate = calculateDynamicRate(secondsElapsed, rnd);
                
                long emitStart = System.currentTimeMillis();
                int recordsToEmit = Math.max(1, (int) Math.round(currentRate * inputRateFactor));
                
                for (int i = 0; i < recordsToEmit; i++) {
                    // Include arrival timestamp and tuple number for tracking
                    long arrivalTime = System.currentTimeMillis();
                    int accountId = rnd.nextInt(10_000);
                    String accountKey = "ACC" + accountId;
                    
                    MLScoringRecord record = new MLScoringRecord(
                            accountKey,
                            id++,
                            1 + rnd.nextDouble() * 499,    // amount
                            rnd.nextInt(2_000),            // merchant
                            null,                          // features (to be filled later)
                            0.0,                          // score (to be calculated)
                            Source_Output,                 // output operator
                            arrivalTime,                   // arrival time for latency tracking
                            id);                          // tuple number
                    
                    ctx.collect(new Tuple2<>(accountKey, record));
                }
                
                long elapsed = System.currentTimeMillis() - emitStart;
                if (elapsed < 1_000) Thread.sleep(1_000 - elapsed);
            }
        }

        private double calculateDynamicRate(double secondsElapsed, Random rnd) {
            // 1. Sine curve: f(t) = amplitude * sin(2πt/period) + baseline
            double sineComponent = Math.sin(2 * Math.PI * secondsElapsed / sinePeriod);
            double baseSineRate = sineAmplitude * sineComponent + sineBaseline;
            
            // 2. Short-term spikes (random bursts)
            double spikeComponent = 1.0;
            if (rnd.nextDouble() < spikeProbability) {
                spikeComponent = spikeMultiplier;
            }
            
            // 3. Short-term Gaussian fluctuation
            double fluctuationComponent = 1.0 + rnd.nextGaussian() * fluctuationStd;
            
            // Combine all components
            double combinedRate = baseSineRate * spikeComponent * fluctuationComponent;
            
            // Ensure rate stays positive and reasonable (min 1, max 10x baseline)
            return Math.max(1.0, Math.min(combinedRate, sineBaseline * 10.0));
        }

        @Override
        public void cancel() { running = false; }
    }
    
    /* ===========================================================
     *  DelayUtil - Same as LinearRoad
     * ===========================================================
     */
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
    
    /* ===========================================================
     *  2 | ParseTxn FlatMap - Following LinearRoad pattern
     * ===========================================================
     */
    public static class ParseTxn extends RichFlatMapFunction<Tuple2<String, MLScoringRecord>, Tuple2<String, MLScoringRecord>> {
        private final long delayMs;
        
        public ParseTxn(long delayMs) {
            this.delayMs = delayMs;
        }
        
        @Override
        public void flatMap(Tuple2<String, MLScoringRecord> input, Collector<Tuple2<String, MLScoringRecord>> out) {
            MLScoringRecord inputRecord = input.f1;
            String accountKey = input.f0;
            
            DelayUtil.delay(delayMs);
            
            long currentTime = System.currentTimeMillis();
            long latency = currentTime - inputRecord.getArrivalTime();
            
            // Print ground truth latency following LinearRoad pattern
            System.out.println("GT: " + inputRecord.getAccountId() + ", " + currentTime + ", " + latency + ", " + inputRecord.getTupleNumber());
            
            // Create updated record
            MLScoringRecord outputRecord = new MLScoringRecord(
                    inputRecord.getAccountId(),
                    inputRecord.getId(),
                    inputRecord.getAmount(),
                    inputRecord.getMerchant(),
                    inputRecord.getFeatures(),
                    inputRecord.getScore(),
                    ParseTxn_Output,
                    inputRecord.getArrivalTime(),
                    inputRecord.getTupleNumber());
            
            out.collect(new Tuple2<>(accountKey, outputRecord));
        }
    }

    /* ===========================================================
     *  3 | FeatureBuilder FlatMap
     * ===========================================================
     */
    public static class FeatureBuilder extends RichFlatMapFunction<Tuple2<String, MLScoringRecord>, Tuple2<String, MLScoringRecord>> {
        private final long delayMs;
        
        public FeatureBuilder(long delayMs) {
            this.delayMs = delayMs;
        }
        
        @Override
        public void flatMap(Tuple2<String, MLScoringRecord> input, Collector<Tuple2<String, MLScoringRecord>> out) {
            MLScoringRecord inputRecord = input.f1;
            String accountKey = input.f0;
            
            String features = inputRecord.getAmount() + "|" + inputRecord.getMerchant();
            DelayUtil.delay(delayMs);
            
            long currentTime = System.currentTimeMillis();
            long latency = currentTime - inputRecord.getArrivalTime();
            
            // Print ground truth latency
            System.out.println("GT: " + inputRecord.getAccountId() + ", " + currentTime + ", " + latency + ", " + inputRecord.getTupleNumber());
            
            // Create updated record with features
            MLScoringRecord outputRecord = new MLScoringRecord(
                    inputRecord.getAccountId(),
                    inputRecord.getId(),
                    inputRecord.getAmount(),
                    inputRecord.getMerchant(),
                    features,
                    inputRecord.getScore(),
                    FeatureBuilder_Output,
                    inputRecord.getArrivalTime(),
                    inputRecord.getTupleNumber());
            
            out.collect(new Tuple2<>(accountKey, outputRecord));
        }
    }

    /* ===========================================================
     *  4 | Feature-Based Realistic GBDT Scorer FlatMap
     * ===========================================================
     */
    public static class RealisticGBDTScorer extends RichFlatMapFunction<Tuple2<String, MLScoringRecord>, Tuple2<String, MLScoringRecord>> {

        private final long baseDelayMs;
        private final double complexityFactor;
        private final Random random;

        public RealisticGBDTScorer(long baseDelayMs, double complexityFactor) {
            this.baseDelayMs = baseDelayMs;
            this.complexityFactor = complexityFactor;
            this.random = new Random(42); // Deterministic for reproducibility
        }

        @Override
        public void flatMap(Tuple2<String, MLScoringRecord> input, Collector<Tuple2<String, MLScoringRecord>> out) throws Exception {
            MLScoringRecord inputRecord = input.f1;
            String accountKey = input.f0;
            
            int accountId = Integer.parseInt(inputRecord.getAccountId().substring(3)); // Remove "ACC" prefix
            double amount = inputRecord.getAmount();
            int merchant = inputRecord.getMerchant();
            
            // Simulate feature-based complexity
            long processingTime = calculateProcessingTime(accountId, amount, merchant);
            
            // Busy wait to simulate processing (processingTime is in microseconds)
            long start = System.nanoTime();
            while (System.nanoTime() - start < processingTime * 1_000L) { /* spin */ }

            // Generate score based on features (more realistic)
            double score = calculateScore(accountId, amount, merchant);
            
            long currentTime = System.currentTimeMillis();
            long latency = currentTime - inputRecord.getArrivalTime();
            
            // Print ground truth latency
            System.out.println("GT: " + inputRecord.getAccountId() + ", " + currentTime + ", " + latency + ", " + inputRecord.getTupleNumber());
            
            // Create updated record with score
            MLScoringRecord outputRecord = new MLScoringRecord(
                    inputRecord.getAccountId(),
                    inputRecord.getId(),
                    inputRecord.getAmount(),
                    inputRecord.getMerchant(),
                    inputRecord.getFeatures(),
                    score,
                    RealisticGBDTScorer_Output,
                    inputRecord.getArrivalTime(),
                    inputRecord.getTupleNumber());
            
            out.collect(new Tuple2<>(accountKey, outputRecord));
        }

        private long calculateProcessingTime(int accountId, double amount, int merchant) {
            // Base processing time (in microseconds)
            long processingTime = baseDelayMs;
            
            // 1. Amount-based complexity (larger amounts need more analysis)
            if (amount > 1000) processingTime += (long)(complexityFactor * 3000); // 3ms in microseconds
            else if (amount > 500) processingTime += (long)(complexityFactor * 2000); // 2ms in microseconds
            else if (amount > 100) processingTime += (long)(complexityFactor * 1000); // 1ms in microseconds
            
            // 2. Account history complexity (simulate account risk profiling)
            int accountComplexity = Math.abs(accountId) % 100;
            if (accountComplexity > 90) processingTime += (long)(complexityFactor * 5000); // 5ms in microseconds
            else if (accountComplexity > 70) processingTime += (long)(complexityFactor * 3000); // 3ms in microseconds
            else if (accountComplexity < 10) processingTime += (long)(complexityFactor * 2000); // 2ms in microseconds
            
            // 3. Merchant category complexity
            int merchantCategory = merchant % 20;
            if (merchantCategory < 2) processingTime += (long)(complexityFactor * 4000); // 4ms in microseconds
            else if (merchantCategory < 5) processingTime += (long)(complexityFactor * 2000); // 2ms in microseconds
            
            // 4. Add some randomness for model tree traversal variations
            double randomFactor = 0.8 + random.nextGaussian() * 0.2; // 80-120% variation
            processingTime = (long)(processingTime * Math.max(0.1, randomFactor));
            
            return Math.max(1, processingTime);
        }
        
        private double calculateScore(int accountId, double amount, int merchant) {
            // Simulate a fraud detection score [0,1]
            double score = 0.1; // Base score
            
            // Amount-based risk
            if (amount > 1000) score += 0.3;
            if (amount > 5000) score += 0.2;
            
            // Account-based risk
            int accountRisk = Math.abs(accountId) % 100;
            score += accountRisk / 1000.0;
            
            // Merchant-based risk
            int merchantRisk = merchant % 20;
            if (merchantRisk < 2) score += 0.4; // High-risk merchants
            
            // Add some noise
            score += random.nextGaussian() * 0.05;
            
            return Math.max(0.0, Math.min(1.0, score));
        }
    }
    
    /* ===========================================================
     *  5 | Latency Tracking FlatMap - Replaces sink, following LinearRoad pattern
     * ===========================================================
     */
    public static class LatencyTrackingFlatMap extends RichFlatMapFunction<Tuple2<String, MLScoringRecord>, Tuple2<String, MLScoringRecord>> {
        
        @Override
        public void flatMap(Tuple2<String, MLScoringRecord> input, Collector<Tuple2<String, MLScoringRecord>> out) throws Exception {
            MLScoringRecord inputRecord = input.f1;
            String accountKey = input.f0;
            
            long currentTime = System.currentTimeMillis();
            long latency = currentTime - inputRecord.getArrivalTime();
            
            // Print ground truth latency in LinearRoad format
            System.out.println("GT: " + inputRecord.getAccountId() + ", " + currentTime + ", " + latency + ", " + inputRecord.getTupleNumber());
            
            // Create final output record
            MLScoringRecord outputRecord = new MLScoringRecord(
                    inputRecord.getAccountId(),
                    inputRecord.getId(),
                    inputRecord.getAmount(),
                    inputRecord.getMerchant(),
                    inputRecord.getFeatures(),
                    inputRecord.getScore(),
                    LatencyTracking_Output,
                    inputRecord.getArrivalTime(),
                    inputRecord.getTupleNumber());
            
            out.collect(new Tuple2<>(accountKey, outputRecord));
        }
    }
    
    /* ===========================================================
     *  6 | Alternative: GBDTScorer for real model usage
     * ===========================================================
     */
    public static class GBDTScorer extends RichMapFunction<
            Tuple2<String, MLScoringRecord>, Tuple2<String, MLScoringRecord>> {

        private transient SomeGbdtModel model;

        @Override
        public void open(Configuration cfg) {
            String modelPath = cfg.getString("model.path", "/opt/models/gbdt.json");
            this.model = SomeGbdtModel.load(modelPath);
        }

        @Override
        public Tuple2<String, MLScoringRecord> map(Tuple2<String, MLScoringRecord> input) {
            MLScoringRecord inputRecord = input.f1;
            String accountKey = input.f0;
            
            double score = model.predict(inputRecord.getFeatures());
            
            MLScoringRecord outputRecord = new MLScoringRecord(
                    inputRecord.getAccountId(),
                    inputRecord.getId(),
                    inputRecord.getAmount(),
                    inputRecord.getMerchant(),
                    inputRecord.getFeatures(),
                    score,
                    inputRecord.getOutputOperator(),
                    inputRecord.getArrivalTime(),
                    inputRecord.getTupleNumber());
            
            return new Tuple2<>(accountKey, outputRecord);
        }
    }

    /* ===========================================================
     *  7 | Placeholder model API
     * ===========================================================
     */
    public static class SomeGbdtModel {
        public static SomeGbdtModel load(String path) {
            // TODO: implement actual deserialization (e.g., XGBoost4J / LightGBM)
            return new SomeGbdtModel();
        }
        public double predict(String features) {
            // TODO: real scoring logic
            return 0.42;
        }
    }
}
