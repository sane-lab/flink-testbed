package flinkapp.frauddetection.rule;

import flinkapp.frauddetection.transaction.Transaction;

import java.io.Serializable;
import java.util.Arrays;

public class FraudOrNot implements Serializable {
    public boolean isFraud;
    public Transaction transc;

    @Override
    public String toString() {
        boolean GT = transc.getFeature("is_fraud").equals("1");
        return "FraudOrNot{" +
                "judge isFraud=" + isFraud +
                " for transaction " + Arrays.toString(transc.getAttribute().subList(0, 5).toArray()) +
                " actually: " + GT +
                ":: result " + (isFraud == GT ? "AC" : "WA") +
                '}';
    }
}
