package flinkapp.frauddetection.function;

import flinkapp.frauddetection.transaction.PrecessedTransaction;
import flinkapp.frauddetection.transaction.Transaction;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.Random;

public class PreprocessingFunction implements MapFunction<Transaction, PrecessedTransaction> {

    float[] center;
    float[] scale;
    private Random r = new Random();
    private double res;

    public PreprocessingFunction(float[] center, float[] scale) {
        this.center = center;
        this.scale = scale;
    }


    public PreprocessingFunction() {

    }

    @Override
    public PrecessedTransaction map(Transaction transaction) throws Exception {
        double res = 0;
        for (int i = 0; i < 50000; i++) {
            double tmp = (double) i / (r.nextInt(100) + 1.0);
            res += tmp;
        }
        this.res = res;
        return new PrecessedTransaction(transaction, center, scale);
    }

}
