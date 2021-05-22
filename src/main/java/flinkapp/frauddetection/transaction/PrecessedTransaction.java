package flinkapp.frauddetection.transaction;

public class PrecessedTransaction {

    static String[] numericFeatureName = {
            "amt", "zip", "lat", "long", "city_pop", "unix_time", "merch_lat", "merch_long",
            "category_food_dining", "category_gas_transport",
            "category_grocery_net", "category_grocery_pos",
            "category_health_fitness", "category_home", "category_kids_pets",
            "category_misc_net", "category_misc_pos", "category_personal_care",
            "category_shopping_net", "category_shopping_pos", "category_travel",
            "gender_M", "age", "dist"
    };

    float[] features;

    public Transaction originalTransaction;


    public PrecessedTransaction(Transaction originalTransaction, float[] center, float[] scale) {
        this.originalTransaction = originalTransaction;
        features = new float[numericFeatureName.length];
        for (int i = 0; i < numericFeatureName.length; i++) {
            String value = originalTransaction.getFeature(numericFeatureName[i]);
            features[i] = Float.parseFloat(value);
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
