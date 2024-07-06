package stockv2;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;

public class SSERealRateSourceFunction extends RichParallelSourceFunction<Quote> {

    private volatile boolean running = true;

    private static final int ORDER_NO = 0;
    private static final int TRAN_MAINT_CODE = 1;
    private static final int LAST_UPD_DATE = 5;
    private static final int LAST_UPD_TIME = 6;
    private static final int ORDER_PRICE = 8;
    private static final int ORDER_EXEC_VOL = 9;
    private static final int ORDER_VOL = 10;
    private static final int SEC_CODE = 11;

    private static final int ACCT_ID = 13;
    private static final int TRADE_DIR = 22;

    private static final String FILTER_KEY1 = "D";
    private static final String FILTER_KEY2 = "X";
    private static final String FILTER_KEY3 = "";

    private final String filePath;
    private final int perKeyStateSize;

    public SSERealRateSourceFunction(String filePath, int perKeyStateSize) {
        this.filePath = filePath;
        this.perKeyStateSize = perKeyStateSize;
    }

    @Override
    public void run(SourceContext<Quote> ctx) throws Exception {
        long start = System.currentTimeMillis();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HH:mm:ss");

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String sCurrentLine;

            while (running && (sCurrentLine = br.readLine()) != null) {
                if (sCurrentLine.equals("end")) {
                    // Simulate a small delay to control the output rate
                    long cur = System.currentTimeMillis();
                    if (cur < start + 50) {
                        Thread.sleep((start + 50) - cur);
                    }
                    start += 50;
                    continue;
                }

                String[] fields = sCurrentLine.split("\\|");
                if (fields.length < 12 || fields[TRAN_MAINT_CODE].equals(FILTER_KEY2) || fields[TRAN_MAINT_CODE].equals(FILTER_KEY3)) {
                    continue;
                }

                long timestamp = sdf.parse(fields[LAST_UPD_DATE] + " " + fields[LAST_UPD_TIME]).getTime();
                String stockId = fields[SEC_CODE];
                String acctId = fields[ACCT_ID];
                double price = Double.parseDouble(fields[ORDER_PRICE]);
                double volume = Double.parseDouble(fields[ORDER_VOL]);
                String buySellIndicator = fields[TRADE_DIR];
//                String payload = StringUtils.repeat("A", perKeyStateSize);
                String payload = "";

                Quote quote = new Quote(stockId, acctId, price, volume, buySellIndicator, timestamp, payload);

                // Synchronize and collect the quote
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(quote);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
