package flinkapp.frauddetection.rule;

import flinkapp.frauddetection.transaction.PrecessedTransaction;
import flinkapp.frauddetection.transaction.Transaction;

import java.io.Serializable;

public class NoRule extends Rule{

    private static final NoRule INSTANCE = new NoRule();

    private NoRule() {
    }

    public static NoRule getINSTANCE() {
        return INSTANCE;
    }

    @Override
    public FraudOrNot isFraud(PrecessedTransaction transaction) {
        FraudOrNot res = new FraudOrNot();
        res.isFraud = true;
        res.transc = transaction.originalTransaction;
        return res;
    }
}
