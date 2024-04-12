package flinkapp.tpcds;

import Nexmark.sources.Util;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class q40 {
    public static void main(String[] args) throws Exception {
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        env.setStateBackend(new MemoryStateBackend(1073741824));
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);



        env.execute();
    }

    private static class catalog_salesSource extends RichParallelSourceFunction<Tuple7<String, String, String, String, Double, Long, Long>> {

        private volatile boolean running = true;
        private static final int cs_sold_date_sk = 0;
        private static final int cs_sold_time_sk = 1;
        private static final int cs_warehouse_sk = 14;
        private static final int cs_item_sk = 15;
        private static final int cs_order_number = 17;
        private static final int cs_sales_price = 21;

        private final String FILE;
        private final long warmup, warmp_rate;

        public catalog_salesSource(String FILE, long warmup, long warmup_rate, long skipCount) {
            this.FILE = FILE;
            this.warmup = warmup;
            this.warmp_rate = warmup_rate;
        }

        @Override
        public void run(SourceContext<Tuple7<String, String, String, String, Double, Long, Long>> ctx) throws Exception {
            String sCurrentLine;
            List<String> textList = new ArrayList<>();
            FileReader stream = null;
            // // for loop to generate message
            BufferedReader br = null;
            int sent_sentences = 0;
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
                    String key = "A" + (count % 10000);
                    ctx.collect(Tuple7.of(key, "A", "B", "C", 0.0, System.currentTimeMillis(), (long)count));
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
                    if (sCurrentLine.equals("end")) {
                        sleepCnt++;
                        if (counter == 0) {
                            noRecSleepCnt++;
                            System.out.println("no record in this sleep !" + noRecSleepCnt);
                        }
                        // System.out.println("output rate: " + counter);
                        counter = 0;
                        cur = System.currentTimeMillis();
                        if (cur < sleepCnt * 50 + start) {
                            Thread.sleep((sleepCnt * 50 + start) - cur);
                        } else {
                            System.out.println("rate exceeds" + 50 + "ms.");
                        }
//                    start = System.currentTimeMillis();
                    }

                    if (sCurrentLine.split("\\|").length < 10) {
                        continue;
                    }

                    Long ts = System.currentTimeMillis();
                    String msg = sCurrentLine;
                    List<String> csArr = Arrays.asList(msg.split("\\|"));
                    ctx.collect(new Tuple7<>(csArr.get(cs_item_sk), csArr.get(cs_order_number), csArr.get(cs_warehouse_sk), csArr.get(cs_sold_date_sk), Double.parseDouble(csArr.get(cs_sales_price)), ts, (long) count));
                    count++;

                    counter++;
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if(stream != null) stream.close();
                    if(br != null) br.close();
                } catch(IOException ex) {
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
}
