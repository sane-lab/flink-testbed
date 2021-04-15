package flinkapp.frauddetection.transaction;

public class PrecessedTransaction {

    static String[] numericFeatureName = {"amt", "gender", "zip", "lat", "long", "city_pop", "unix_time", "merch_lat", "merch_long"};

    float[] features;

    public Transaction originalTransaction;


    public PrecessedTransaction(Transaction originalTransaction, float[] center, float[] scale) {
        this.originalTransaction = originalTransaction;
        features = new float[numericFeatureName.length];
        for (int i = 0; i < numericFeatureName.length; i++) {
            String value = originalTransaction.getFeature(numericFeatureName[i]);
            if (numericFeatureName[i].equals("gender")) {
                switch (value) {
                    case "F":
                        features[i] = 1;
                        break;
                    case "M":
                        features[i] = 0;
                        break;
                }
            } else {
                features[i] = Float.parseFloat(value);
            }
        }
        if (center == null || scale == null) {
            return;
        }
        for (int i = 0; i < features.length; i++) {
            features[i] = (features[i] - center[i]) / scale[i];
        }
    }

    public float getFeature(int index) {
        return features[index];
    }
}
