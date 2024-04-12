package flinkapp.tpcds;

import flinkapp.StreamSluiceTestSet.StockAnalysisApplication;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class q40 {
    public static void main(String[] args) throws Exception {
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        env.setStateBackend(new MemoryStateBackend(1073741824));
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        DataStreamSource<Tuple6<String, String, Double, Double, Long, Long>> source = env.addSource(new StockAnalysisApplication.SSERealRateSourceWithVolume(params.get("file_name", "/home/samza/SSE_data/sb-4hr-50ms.txt"), params.getLong("warmup_time", 30L) * 1000, params.getLong("warmup_rate", 1500L), params.getLong("skip_interval", 20L) * 20))
                .setParallelism(params.getInt("p1", 1));
        DataStream<Tuple5<String, Double, Double, Long, Long>> up = source
                .keyBy(0)
                .flatMap(new StockAnalysisApplication.StockPreprocess(params.getInt("op2Delay", 1000)))
                .disableChaining()
                .name("Preprocess")
                .uid("op2")
                .setParallelism(params.getInt("p2", 1))
                .setMaxParallelism(params.getInt("mp2", 8))
                .slotSharingGroup("g2");
        DataStream<Tuple5<String, Double, Integer, Long, Long>> split1 = up
                .keyBy(0)
                .flatMap(new StockAnalysisApplication.PriceAnalysis(params.getInt("op3Delay", 4000)))
                .disableChaining()
                .name("Price Analysis")
                .uid("op3")
                .setParallelism(params.getInt("p3", 1))
                .setMaxParallelism(params.getInt("mp3", 8))
                .slotSharingGroup("g3");
        DataStream<Tuple5<String, Double, Integer, Long, Long>> split2 = up
                .keyBy(0)
                .flatMap(new StockAnalysisApplication.VolumeFiltering(params.getInt("op4Delay", 1000)))
                .disableChaining()
                .name("Volume Filtering")
                .uid("op4")
                .setParallelism(params.getInt("p4", 1))
                .setMaxParallelism(params.getInt( "mp4", 8))
                .slotSharingGroup("g4")
                .keyBy(0)
                .flatMap(new StockAnalysisApplication.VolumeAggregation(params.getInt("op5Delay", 2000)))
                .disableChaining()
                .name("Volume Aggregation")
                .uid("op5")
                .setParallelism(params.getInt("p5", 1))
                .setMaxParallelism(params.getInt( "mp5", 8))
                .slotSharingGroup("g5");
        DataStream<Tuple5<String, Double, Integer, Long, Long>> unionStream =  split1.union(split2);
        DataStream<Tuple5<String, Double, Double, Long, Long>> joined = unionStream.keyBy(0)
                .flatMap(new StockAnalysisApplication.JoinMap())
                .disableChaining()
                .name("Join")
                .uid("op6")
                .setParallelism(params.getInt("p6", 1))
                .setMaxParallelism(params.getInt("mp6", 8))
                .slotSharingGroup("g6");

//        DataStream<Tuple5<String, Double, Double, Long, Long>> joined = ((SingleOutputStreamOperator)(split1
//                .join(split2)
//                .where((KeySelector<Tuple4<String, Double, Long, Long>, Long>) p -> p.f3)
//                .equalTo((KeySelector<Tuple4<String, Double, Long, Long>, Long>) p -> p.f3)
//                .window(TumblingEventTimeWindows.of(Time.milliseconds(1000)))
//                .apply(new JoinMap())))
//                .disableChaining()
//                .name("Join")
//                .uid("op6")
//                .setParallelism(params.getInt("p6", 1))
//                .setMaxParallelism(params.getInt("mp6", 8))
//                .slotSharingGroup("g6");

        joined.keyBy(0)
                .map(new StockAnalysisApplication.Analysis(params.getInt("op7Delay", 3000)))
                .disableChaining()
                .name("Analysis")
                .uid("op7")
                .setParallelism(params.getInt("p7", 1))
                .setMaxParallelism(params.getInt("mp7", 8))
                .slotSharingGroup("g7");

        env.execute();
    }
}
