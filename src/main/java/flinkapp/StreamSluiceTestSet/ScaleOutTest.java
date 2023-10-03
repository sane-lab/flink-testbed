package flinkapp.StreamSluiceTestSet;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ScaleOutTest{
    public static void main(String[] args) throws Exception {
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        env.setStateBackend(new MemoryStateBackend(100000000));
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.enableCheckpointing(1000);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        FlinkKafkaProducer011<String> kafkaProducer = new FlinkKafkaProducer011<String>(
//                "localhost:9092", "my-flink-demo-topic0", new SimpleStringSchema());
//        kafkaProducer.setWriteTimestampToKafka(true);

        final long PHASE1_RATE = params.getLong("phase1Rate", 400);
        final long PHASE2_RATE = params.getLong("phase2Rate", 600);
        final long INTERMEDIATE_RATE = params.getLong("interRate", 500);
        final long PHASE1_TIME = params.getLong("phase1Time", 60) * 1000;
        final long PHASE2_TIME = params.getLong("phase2Time", 60) * 1000;
        final long INTERMEDIATE_TIME = params.getLong("interTime", 120) * 1000;
        final long INTERMEDIATE_PERIOD = params.getLong("interPeriod", 240) * 1000;
        final int p1 = params.getInt("p1", 1);
        final int mp1 = params.getInt("mp1", 64);
        final double zipf_skew = params.getDouble("zipf_skew", 0);
        env.addSource(new TwoPhaseSineSource(PHASE1_TIME, PHASE2_TIME, INTERMEDIATE_TIME, PHASE1_RATE, PHASE2_RATE, INTERMEDIATE_RATE, INTERMEDIATE_PERIOD, mp1, zipf_skew))
                .keyBy(0)
                .map(new DumbStatefulMap(5))
                .disableChaining()
                .name("Time counter")
                .setMaxParallelism(mp1)
                .setParallelism(p1)
                .slotSharingGroup("a");

        env.execute();
    }

}
