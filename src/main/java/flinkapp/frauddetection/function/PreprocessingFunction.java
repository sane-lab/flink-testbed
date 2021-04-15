package flinkapp.frauddetection.function;

import flinkapp.frauddetection.transaction.PrecessedTransaction;
import flinkapp.frauddetection.transaction.Transaction;
import org.apache.flink.api.common.functions.MapFunction;

public class PreprocessingFunction implements MapFunction<Transaction, PrecessedTransaction> {

    float[] center;
    float[] scale;

    public PreprocessingFunction(float[] center, float[] scale) {
        this.center = center;
        this.scale = scale;
    }


    public PreprocessingFunction() {

    }

    @Override
    public PrecessedTransaction map(Transaction transaction) throws Exception {
        return new PrecessedTransaction(transaction, center, scale);
    }

}
