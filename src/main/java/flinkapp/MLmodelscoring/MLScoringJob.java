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
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;

/**
 * Fraud-style ML scoring pipeline (mock).
 *
 *  src ─► Parse ▶ FeatureBuild ▶ RealisticGBDTScorer ▶ LatencyTrackingSink ─► file
 *
 *  ▪ Key = userId (String)                              ▪ State backend = RocksDB
 *  ▪ Feature-based processing time simulation           ▪ All parameters overridable via –Dflags
 *  ▪ Ground truth latency tracked and written to file  ▪ Similar to LinearRoad pattern
 */
public final class MLScoringJob {

    public static void main(String[] args) throws Exception {

        // ---------- 1. env & params ----------
        ParameterTool p = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(p);
        env.setStateBackend(new MemoryStateBackend(100000000));
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // env.enableCheckpointing(p.getLong("ckpt", 10_000));

        // parameters - now tunable via command line
        long runSeconds = p.getLong("run.seconds", 300);       // 5 min demo
        int  baseRate   = p.getInt("base.rate", 500);          // 500 txn/s base rate
        
        DataStream<String> results = env
                .addSource(new TxnSource(
                    runSeconds * 1_000L, 
                    baseRate,
                    p.getDouble("sine.amplitude", 0.3),      // sine wave amplitude (0.3 = ±30% variation)
                    p.getDouble("sine.period", 60.0),        // sine wave period in seconds
                    p.getDouble("spike.probability", 0.05),  // probability of spike per second
                    p.getDouble("spike.multiplier", 3.0),    // spike multiplier (3x normal rate)
                    p.getDouble("fluctuation.std", 0.1)      // short-term fluctuation std dev (10%)
                ))
                .name("Mock-Txn-Source")
                .setParallelism(p.getInt("p1", 1))
                // → POJO (remove the problematic keyBy(0))
                .map(new ParseTxn(p.getLong("parse.delay", 1))).name("ParseTxn").uid("parse_txn")
                .disableChaining()
                .setParallelism(p.getInt("p2", 1))
                .setMaxParallelism(p.getInt("mp2", 8))
                .slotSharingGroup("g2")
                // simple keyBy(accountId) for partitioning
                .keyBy(t -> t.accountId)
                // build features
                .map(new FeatureBuilder(p.getLong("feature.delay", 1))).name("FeatureBuilder")
                .uid("feature_builder")
                .disableChaining()
                .setParallelism(p.getInt("p3", 1))
                .setMaxParallelism(p.getInt("mp3", 8))
                .slotSharingGroup("g3")
                // ↓ Feature-based realistic GBDT scorer
                .flatMap(new RealisticGBDTScorer(
                    p.getLong("scorer.base.delay", 2),           // Base processing time (ms)
                    p.getDouble("scorer.complexity.factor", 1.0) // Complexity multiplier
                ))
                .uid("scorer")
                .setParallelism(p.getInt("p4", 1))
                .setMaxParallelism(p.getInt("mp4", 8))
                .slotSharingGroup("g4")
                .disableChaining();
                
        // Add ground truth latency tracking sink similar to LinearRoad
        results.addSink(new LatencyTrackingSink(p.getString("latency.output.file", "/tmp/ml_scoring_latency.log")))
                .name("Latency Tracking Sink")
                .uid("latency_sink")
                .setParallelism(1); // Single parallelism to avoid file conflicts
                
        env.execute("Real-Time ML Scoring (Mock)");
    }
    
    /* ===========================================================
     *  1 | Synthetic source producing CSV strings with dynamic rate
     * ===========================================================
     */
    public static class TxnSource implements SourceFunction<String> {

        private final long runMillis;
        private final int baseRate;
        private final double sineAmplitude;
        private final double sinePeriod;
        private final double spikeProbability;
        private final double spikeMultiplier;
        private final double fluctuationStd;
        private volatile boolean running = true;

        public TxnSource(long runMillis, int baseRate, double sineAmplitude, double sinePeriod, 
                        double spikeProbability, double spikeMultiplier, double fluctuationStd) {
            this.runMillis = runMillis;
            this.baseRate = baseRate;
            this.sineAmplitude = sineAmplitude;
            this.sinePeriod = sinePeriod;
            this.spikeProbability = spikeProbability;
            this.spikeMultiplier = spikeMultiplier;
            this.fluctuationStd = fluctuationStd;
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            Random rnd = new Random(114514);
            long start = System.currentTimeMillis();
            long id = 0L;

            while (running && System.currentTimeMillis() - start < runMillis) {
                long currentTime = System.currentTimeMillis() - start;
                double secondsElapsed = currentTime / 1000.0;
                
                // Calculate dynamic rate with multiple components
                double currentRate = calculateDynamicRate(secondsElapsed, rnd);
                
                long emitStart = System.currentTimeMillis();
                int recordsToEmit = Math.max(1, (int) Math.round(currentRate));
                
                for (int i = 0; i < recordsToEmit; i++) {
                    // Include arrival timestamp and tuple number in the CSV for tracking
                    long arrivalTime = System.currentTimeMillis();
                    String record = String.format(
                            "%d,%d,%.2f,M%d,%d,%d",
                            id++,
                            rnd.nextInt(10_000),          // accountId
                            1 + rnd.nextDouble() * 499,    // amount
                            rnd.nextInt(2_000),            // merchant
                            arrivalTime,                   // arrival time for latency tracking
                            id);                          // tuple number
                    ctx.collect(record);
                }
                
                long elapsed = System.currentTimeMillis() - emitStart;
                if (elapsed < 1_000) Thread.sleep(1_000 - elapsed);
            }
        }

        private double calculateDynamicRate(double secondsElapsed, Random rnd) {
            // 1. Long-term sine wave variation
            double sineComponent = Math.sin(2 * Math.PI * secondsElapsed / sinePeriod);
            double sineVariation = 1.0 + sineAmplitude * sineComponent;
            
            // 2. Short-term spikes (random bursts)
            double spikeComponent = 1.0;
            if (rnd.nextDouble() < spikeProbability) {
                spikeComponent = spikeMultiplier;
            }
            
            // 3. Short-term Gaussian fluctuation
            double fluctuationComponent = 1.0 + rnd.nextGaussian() * fluctuationStd;
            
            // Combine all components
            double combinedRate = baseRate * sineVariation * spikeComponent * fluctuationComponent;
            
            // Ensure rate stays positive and reasonable
            return Math.max(1.0, Math.min(combinedRate, baseRate * 10.0));
        }

        @Override
        public void cancel() { running = false; }
    }

    /* ===========================================================
     *  2 | Txn POJO with latency tracking fields
     * ===========================================================
     */
    public static class Txn {
        public long   id;
        public int    accountId;
        public double amount;
        public int    merchant;
        public long   arrivalTime;   // arrival timestamp for latency tracking
        public long   tupleNumber;   // tuple sequence number

        // Flink requires a no-arg constructor
        public Txn() {}
        public Txn(long id, int acc, double amt, int mer, long arrivalTime, long tupleNumber){
            this.id = id;
            this.accountId = acc;
            this.amount = amt;
            this.merchant = mer;
            this.arrivalTime = arrivalTime;
            this.tupleNumber = tupleNumber;
        }
        @Override public String toString(){
            return String.format("Txn(%d,%d,%.2f,%d,%d,%d)", id, accountId, amount, merchant, arrivalTime, tupleNumber);
        }
    }
    
    private static void delay(long time) {
        try {
            // Add Gaussian fluctuation: mean=0, std=time*0.1 (10% of target time)
            // This ensures the average delay is around the target time
            double gaussianNoise = new Random().nextGaussian() * time * 0.1;
            long actualDelay = Math.max(1, (long) (time + gaussianNoise)); // Ensure minimum 1ms
            Thread.sleep(actualDelay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    
    /* ===========================================================
     *  3 | CSV → Txn with latency tracking
     * ===========================================================
     */
    public static class ParseTxn implements MapFunction<String, Txn> {
        private final long delayMs;
        
        public ParseTxn(long delayMs) {
            this.delayMs = delayMs;
        }
        
        @Override
        public Txn map(String v) {
            String[] f = v.split(",");
            delay(delayMs);
            return new Txn(
                    Long.parseLong(f[0]),      // id
                    Integer.parseInt(f[1]),    // accountId
                    Double.parseDouble(f[2]),  // amount
                    Integer.parseInt(f[3]),    // merchant
                    Long.parseLong(f[4]),      // arrivalTime
                    Long.parseLong(f[5]));     // tupleNumber
        }
    }

    /* ===========================================================
     *  4 | FeatureBuilder with latency tracking
     * ===========================================================
     */
    public static class FeatureBuilder implements
            MapFunction<Txn, Tuple2<Integer,String>> {
        private final long delayMs;
        
        public FeatureBuilder(long delayMs) {
            this.delayMs = delayMs;
        }
        
        @Override
        public Tuple2<Integer,String> map(Txn txn) {
            String features = txn.amount + "|" + txn.merchant + "|" + txn.arrivalTime + "|" + txn.tupleNumber;
            delay(delayMs);
            return Tuple2.of(txn.accountId, features);
        }
    }

    /* ===========================================================
     *  5 | Feature-Based Realistic GBDT Scorer
     * ===========================================================
     */
    public static class RealisticGBDTScorer extends RichFlatMapFunction<
                Tuple2<Integer,String>, String> {

        private final long baseDelayMs;
        private final double complexityFactor;
        private final Random random;

        public RealisticGBDTScorer(long baseDelayMs, double complexityFactor) {
            this.baseDelayMs = baseDelayMs;
            this.complexityFactor = complexityFactor;
            this.random = new Random(42); // Deterministic for reproducibility
        }

        @Override
        public void flatMap(Tuple2<Integer,String> in, Collector<String> out)
                throws Exception {

            int accountId = in.f0;
            String[] features = in.f1.split("\\|");
            double amount = Double.parseDouble(features[0]);
            int merchant = Integer.parseInt(features[1].substring(1)); // Remove 'M' prefix
            long arrivalTime = Long.parseLong(features[2]);
            long tupleNumber = Long.parseLong(features[3]);
            
            // Simulate feature-based complexity
            long processingTime = calculateProcessingTime(accountId, amount, merchant);
            
            // Busy wait to simulate processing
            long start = System.nanoTime();
            while (System.nanoTime() - start < processingTime * 1_000_000L) { /* spin */ }

            // Generate score based on features (more realistic)
            double score = calculateScore(accountId, amount, merchant);
            
            String result = accountId + "," + score + "," + arrivalTime + "," + tupleNumber;
            out.collect(result);
        }

        private long calculateProcessingTime(int accountId, double amount, int merchant) {
            // Base processing time
            long processingTime = baseDelayMs;
            
            // 1. Amount-based complexity (larger amounts need more analysis)
            if (amount > 1000) processingTime += (long)(complexityFactor * 3);
            else if (amount > 500) processingTime += (long)(complexityFactor * 2);
            else if (amount > 100) processingTime += (long)(complexityFactor * 1);
            
            // 2. Account history complexity (simulate account risk profiling)
            int accountComplexity = Math.abs(accountId) % 100;
            if (accountComplexity > 90) processingTime += (long)(complexityFactor * 5); // VIP accounts
            else if (accountComplexity > 70) processingTime += (long)(complexityFactor * 3); // High-risk accounts
            else if (accountComplexity < 10) processingTime += (long)(complexityFactor * 2); // New accounts
            
            // 3. Merchant category complexity
            int merchantCategory = merchant % 20;
            if (merchantCategory < 2) processingTime += (long)(complexityFactor * 4); // High-risk merchants (gambling, crypto)
            else if (merchantCategory < 5) processingTime += (long)(complexityFactor * 2); // Financial services
            
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
     *  6 | Latency Tracking Sink - Similar to LinearRoad GT pattern
     * ===========================================================
     */
    public static class LatencyTrackingSink extends RichSinkFunction<String> {
        
        private final String outputFilePath;
        private transient PrintWriter writer;
        
        public LatencyTrackingSink(String outputFilePath) {
            this.outputFilePath = outputFilePath;
        }
        
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            try {
                FileWriter fileWriter = new FileWriter(outputFilePath, true); // append mode
                writer = new PrintWriter(fileWriter);
            } catch (IOException e) {
                throw new RuntimeException("Failed to open latency output file: " + outputFilePath, e);
            }
        }
        
        @Override
        public void invoke(String result, Context context) throws Exception {
            String[] parts = result.split(",");
            if (parts.length >= 4) {
                int accountId = Integer.parseInt(parts[0]);
                double score = Double.parseDouble(parts[1]);
                long arrivalTime = Long.parseLong(parts[2]);
                long tupleNumber = Long.parseLong(parts[3]);
                
                long currentTime = System.currentTimeMillis();
                long latency = currentTime - arrivalTime;
                
                // Print to console in LinearRoad GT format
                System.out.println("GT: ACC" + accountId + ", " + currentTime + ", " + latency + ", " + tupleNumber);
                
                // Also write to file for offline analysis
                String logEntry = String.format("GT: ACC%d, %d, %d, %d, %.4f%n", 
                    accountId, currentTime, latency, tupleNumber, score);
                writer.print(logEntry);
                writer.flush(); // Ensure immediate write
            }
        }
        
        @Override
        public void close() throws Exception {
            if (writer != null) {
                writer.close();
            }
            super.close();
        }
    }
    
    /* ===========================================================
     *  7 | Alternative: GBDTScorer for real model usage
     * ===========================================================
     */
    public static class GBDTScorer extends RichMapFunction<
            Tuple2<Integer,String>, String> {

        private transient SomeGbdtModel model;

        @Override
        public void open(Configuration cfg) {
            String modelPath = cfg.getString("model.path", "/opt/models/gbdt.json");
            this.model = SomeGbdtModel.load(modelPath);
        }

        @Override
        public String map(Tuple2<Integer,String> in) {
            String[] features = in.f1.split("\\|");
            String featureStr = features[0] + "|" + features[1]; // amount|merchant
            long arrivalTime = Long.parseLong(features[2]);
            long tupleNumber = Long.parseLong(features[3]);
            
            double score = model.predict(featureStr); // model parses feature string
            return in.f0 + "," + score + "," + arrivalTime + "," + tupleNumber;
        }
    }

    /* ===========================================================
     *  8 | Placeholder model API
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
