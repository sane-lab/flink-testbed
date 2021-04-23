package flinkapp.frauddetection.rule;

import flinkapp.frauddetection.transaction.PrecessedTransaction;

public class DecisionTreeRule extends Rule{

    public DecisionTreeRule(int[] feature, float[] thresholds, int[] leftChildren, int[] rightChildren, float[][] value) {
        this.feature = feature;
        this.thresholds = thresholds;
        this.leftChildren = leftChildren;
        this.rightChildren = rightChildren;
        this.value = value;
    }

    //  store which feature of each decision node is refer to
    int[] feature;
    // store the threshold in each decision node
    float[] thresholds;
    // store the left children of each node
    int[] leftChildren;
    // store the right children of each node
    int[] rightChildren;
    // store the possibility of being fraud or non-fraud
    float[][] value;

    public DecisionTreeRule(){
    }

//    @Override
    public FraudOrNot isFraud(PrecessedTransaction transaction) {
        if(feature == null){
            return NoRule.getINSTANCE().isFraud(transaction);
        }
        FraudOrNot fraudOrNot = new FraudOrNot();
        int currNode = 0;
        while (feature[currNode] >= 0){
            float featureValue = transaction.getFeature(feature[currNode]);
            if(featureValue <= thresholds[currNode]){
                currNode = leftChildren[currNode];
            }else {
                currNode = rightChildren[currNode];
            }
        }
        float[] possibility = value[currNode];
        fraudOrNot.isFraud = possibility[0] < possibility[1];
        fraudOrNot.transc = transaction.originalTransaction;
        return fraudOrNot;
    }
}
