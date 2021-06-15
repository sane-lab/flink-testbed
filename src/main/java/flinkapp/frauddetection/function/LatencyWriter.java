package flinkapp.frauddetection.function;

import flinkapp.frauddetection.rule.FraudOrNot;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

public class LatencyWriter extends RichSinkFunction<FraudOrNot> {

    private final String filePath;
    private transient OutputStream outputStream;

    public LatencyWriter(String writingPath) {
        this.filePath = writingPath;

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        try {
            if (Files.exists(Paths.get(filePath))) {
                Files.delete(Paths.get(filePath));
            }
            outputStream = new FileOutputStream(filePath);
        } catch (IOException e) {
            e.printStackTrace();
            outputStream = System.out;
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (outputStream != null && outputStream != System.out) {
            outputStream.close();
        }
    }

    @Override
    public void invoke(FraudOrNot value, Context context) throws Exception {
        String sb = value.recordLatency();
        outputStream.write(sb.getBytes());
    }

}
