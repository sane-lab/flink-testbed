package stockv2;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StockExchangeApp {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().registerKryoType(Quote.class);

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);


        env.enableCheckpointing(params.getInt("interval", 1000));

        int perKeyStateSize = params.getInt("perKeySize", 1024);
        String filepath = params.get("filepath", "/home/myc/workspace/flink-testbed/sb-4hr-50ms.txt");

        final DataStream<Quote> quotes = env
                .addSource(new SSERealRateSourceFunction(filepath, perKeyStateSize))
                .setParallelism(params.getInt("p1", 1));

        quotes
            .keyBy(quote -> quote.acctId)
            .process(new StockExchangeFunction(perKeyStateSize))
            .name("Splitter FlatMap")
            .uid("flatmap")
            .setParallelism(params.getInt("p2", 1))
            .setMaxParallelism(params.getInt("mp2", 8));
//            .print();

        env.execute("Stock Exchange Matchmaking");
    }
}
