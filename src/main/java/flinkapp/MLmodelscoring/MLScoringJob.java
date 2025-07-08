package flinkapp.MLmodelscoring;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * Fraud-style ML scoring pipeline (mock).
 *
 *  src ─► Parse ▶ FeatureBuild ▶ ScoreGBDT ▶ AlertJoin ─► sink
 *
 *  ▪ Key = userId (String)                              ▪ State backend = RocksDB
 *  ▪ Metric latency recorded inside ScoreGBDT           ▪ All parameters overridable via –Dflags
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
        env
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
                .keyBy(0)
                // → POJO
                .map(new ParseTxn(p.getLong("parse.delay", 1))).name("ParseTxn").uid("parse_txn")
                .disableChaining()
                .setParallelism(p.getInt("p2", 1))
                .setMaxParallelism(p.getInt("mp2", 8))
                .slotSharingGroup("g2")
                // simple keyBy(accountId) just to show partitioning
                .keyBy(t -> t.accountId)
                // build features
                .map(new FeatureBuilder(p.getLong("feature.delay", 1))).name("FeatureBuilder")
                .uid("feature_builder")
                .disableChaining()
                .setParallelism(p.getInt("p3", 1))
                .setMaxParallelism(p.getInt("mp3", 8))
                .slotSharingGroup("g3")
                // ↓ here you could call your GBDT model scorer
                .flatMap(new DummyScorer())   // placeholder
                .uid("scorer")
                .setParallelism(p.getInt("p4", 1))
                .setMaxParallelism(p.getInt("mp4", 8))
                .slotSharingGroup("g4")
                .disableChaining();
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
                    String record = String.format(
                            "%d,%d,%.2f,M%d,%d",
                            id++,
                            rnd.nextInt(10_000),          // accountId
                            1 + rnd.nextDouble() * 499,    // amount
                            rnd.nextInt(2_000),            // merchant
                            System.currentTimeMillis());   // event-time
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
     *  2 | Txn POJO
     * ===========================================================
     */
    public static class Txn {
        public long   id;
        public int    accountId;
        public double amount;
        public int    merchant;
        public long   ts;   // event-time ms

        // Flink requires a no-arg constructor
        public Txn() {}
        public Txn(long id,int acc,double amt,int mer,long ts){
            this.id=id;this.accountId=acc;this.amount=amt;this.merchant=mer;this.ts=ts;
        }
        @Override public String toString(){
            return String.format("Txn(%d,%d,%.2f,%d,%d)",id,accountId,amount,merchant,ts);
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
     *  3 | CSV → Txn
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
                    Long.parseLong(f[0]),
                    Integer.parseInt(f[1]),
                    Double.parseDouble(f[2]),
                    Integer.parseInt(f[3]),
                    Long.parseLong(f[4]));
        }
    }

    /* ===========================================================
     *  4 | FeatureBuilder  (Txn → Tuple2<accountId, rawFeatureStr>)
     *     Very simple: "<amount>|<merchant>"
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
            String features = txn.amount + "|" + txn.merchant;
            delay(delayMs);
            return Tuple2.of(txn.accountId, features);
        }
    }

    /* ===========================================================
     *  5a | DummyScorer – injects key-dependent compute cost
     *      Heavy key: accountId % 10 == 0  → 5 ms busy loop
     *      Light key: others               → 0.5 ms busy loop
     * ===========================================================
     */
    public static class DummyScorer extends RichFlatMapFunction<
                Tuple2<Integer,String>, String> {

        @Override
        public void flatMap(Tuple2<Integer,String> in, Collector<String> out)
                throws Exception {

            int key = in.f0;
            // heavy keys every 10th account
            long busyNanos = (key % 10 == 0) ? 5_000_000L : 500_000L;
            long start = System.nanoTime();
            while (System.nanoTime() - start < busyNanos) { /* spin */ }

            // fake score ∈ [0,1)
            double score = (key * 0.6180339887) % 1.0;
            out.collect(key + "," + score);
        }
    }
    /* ===========================================================
     *  5b | GBDTScorer – real model load (needs SomeGbdtModel)
     *      Replace DummyScorer with this in the pipeline
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
            double score = model.predict(in.f1); // model parses feature string
            return in.f0 + "," + score;
        }
    }

    /* ===========================================================
     *  6 | Placeholder model API – adapt to your library
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
