package flinkapp.frauddetection;

import flinkapp.frauddetection.function.FileReadingFunction;
import flinkapp.frauddetection.function.PreprocessingFunction;
import flinkapp.frauddetection.function.ProcessingFunction;
import flinkapp.frauddetection.rule.DecisionTreeRule;
import flinkapp.frauddetection.rule.FraudOrNot;
import flinkapp.frauddetection.transaction.PrecessedTransaction;
import flinkapp.frauddetection.transaction.Transaction;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import sun.misc.IOUtils;

import javax.tools.JavaCompiler;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class RuleEvaluatorV2 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.disableOperatorChaining();
        // get transaction
        final DataStream<Transaction> transactionDataStream = getSourceStream(env);
        // some preprocessing needed
        final DataStream<PrecessedTransaction> preprocessedStream = transactionDataStream.map(new PreprocessingFunction())
                .name("preprocess")
                .setParallelism(4);
        // start processing data
        DataStream<FraudOrNot> resultStream = preprocessedStream.process(new ProcessingFunction(new DecisionTreeRule()))
                .name("dtree")
                .setParallelism(4);
        // just print here
        resultStream.print();
        System.out.println(env.getExecutionPlan());
        env.execute();

        RuleEvaluatorV2 v2 = new RuleEvaluatorV2();
        byte[] classData = v2.javaCompilerTest();
        v2.loadClass(classData);
    }

    private static DataStream<Transaction> getSourceStream(StreamExecutionEnvironment env) {
        return env.addSource(
                new FileReadingFunction(
//                        RuleEvaluatorV2.class.getClassLoader().getResource("fraudTest.csv").getPath()))
                        "/home/hya/prog/flink-testbed/src/main/resources/fraudTest.csv"))
                .uid("sentence-source")
                .setParallelism(1);
    }

    private byte[] javaCompilerTest() throws IOException {
        String className = getClass().getPackage().getName() + ".Foo";
        String sourceCode = "package flinkapp.frauddetection;\n" +
                "\n" +
                "public class Foo {\n" +
                "    public static void Hello(){\n" +
                "        System.out.println(\"hello world!\");\n" +
                "    }\n" +
                "}\n";

        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();

        List<JavaSourceFromString> unitsToCompile = new ArrayList<JavaSourceFromString>() {{
            add(new JavaSourceFromString(className, sourceCode));
        }};

        StandardJavaFileManager fileManager = compiler.getStandardFileManager(null, null, null);
        compiler.getTask(null, fileManager, null, null, null, unitsToCompile)
                .call();
        fileManager.close();

        // My question is: is it possible to compile straight to a byte[] array, and avoid the messiness of dealing with File I/O altogether?
        // see https://stackoverflow.com/questions/2130039/javacompiler-from-jdk-1-6-how-to-write-class-bytes-directly-to-byte-array
        FileInputStream fis = new FileInputStream("Foo.class");

        return IOUtils.readAllBytes(fis);
    }

    //Define Custom ClassLoader
    public static class ByteClassLoader extends ClassLoader {
        private final HashMap<String, byte[]> byteDataMap = new HashMap<>();

        public ByteClassLoader(ClassLoader parent) {
            super(parent);
        }

        public void loadDataInBytes(byte[] byteData, String resourcesName) {
            byteDataMap.put(resourcesName, byteData);
        }

        @Override
        protected Class<?> findClass(String className) throws ClassNotFoundException {
            if (byteDataMap.isEmpty())
                throw new ClassNotFoundException("byte data is empty");

            String filePath = className.replaceAll("\\.", "/").concat(".class");
            byte[] extractedBytes = byteDataMap.get(className);
            if (extractedBytes == null)
                throw new ClassNotFoundException("Cannot find " + filePath + " in bytes");

            return defineClass(className, extractedBytes, 0, extractedBytes.length);
        }
    }

    //Example Usage
    public void loadClass(byte[] byteData) throws IOException, ClassNotFoundException, InvocationTargetException, IllegalAccessException {

        ByteClassLoader byteClassLoader = new ByteClassLoader(this.getClass().getClassLoader());
        //Load bytes into hashmap
        byteClassLoader.loadDataInBytes(byteData, getClass().getPackage().getName() + ".Foo");

        Class<?> helloWorldClass = byteClassLoader.loadClass(getClass().getPackage().getName() + ".Foo");
        helloWorldClass.getMethods()[0].invoke(null);
    }
}
