package flinkapp.frauddetection.function;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;

public class FileWritingFunction extends RichSinkFunction<Tuple2<String, Integer>> {

    private final String filePath;
    private transient OutputStream outputStream;

    public FileWritingFunction(String writingPath) {
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
            outputStream.write("timestamp,type,count\n".getBytes());
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
    public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
        String sb = String.valueOf(context.currentProcessingTime() / 1000) +
                ',' +
                value.f0 +
                ',' +
                value.f1 + '\n';
        outputStream.write(sb.getBytes());
    }

}
