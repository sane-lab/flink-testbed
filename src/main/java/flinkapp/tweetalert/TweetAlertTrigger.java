package flinkapp.tweetalert;

import org.apache.flink.api.java.tuple.Tuple19;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TweetAlertTrigger {
    private static final int Source_Output = 0;
    private static final int SentimentAnalysis_Output = 1;
    private static final int InfluenceScoring_Output = 3;
    private static final int ContentCategorization_Output = 4;
    private static final int Join_Output = 5;
    private static final int Aggregation_Output = 6;
    private static final int AlertTrigger_Output = 7;

    public static void main(String[] args) throws Exception {
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        env.setStateBackend(new MemoryStateBackend(1073741824));
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
//        DataStreamSource<Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> source =
//                env.addSource(new TweetSource(params.get("file_name", "/home/samza/Tweet_data/3hr.txt"),
//                                params.getLong("warmup_time", 30L) * 1000,
//                                params.getLong("warmup_rate", 1500L),
//                                params.getLong("skip_interval", 0L) * 20))
//                        .setParallelism(params.getInt("p1", 1));
//
//        DataStream<Tuple19<String, Integer, String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Long, Long>> afterAccidentDetection = source
//                .keyBy(TweetSource.)
//                .flatMap(new SentimentAnalysis(params.getInt("op2Delay", 1000)))
//                .disableChaining()
//                .name("Accident Detection")
//                .uid("op2")
//                .setParallelism(params.getInt("p2", 1))
//                .setMaxParallelism(params.getInt("mp2", 8))
//                .slotSharingGroup("g2");
//        afterAccidentDetection.union(source)
//                .keyBy(LinearRoad.LinearRoadSource.Car_ID) //.keyBy(LinearRoadSource.Seg_ID)
//                .map(new LinearRoad.AccidentNotification(params.getInt("op3Delay", 1000)))
//                .disableChaining()
//                .name("Accident Notification")
//                .uid("op3")
//                .setParallelism(params.getInt("p3", 1))
//                .setMaxParallelism(params.getInt("mp3", 8))
//                .slotSharingGroup("g3");
        env.execute();
    }


}

