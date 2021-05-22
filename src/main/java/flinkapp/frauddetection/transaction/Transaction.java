/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package flinkapp.frauddetection.transaction;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;

public class Transaction implements Serializable {

    static String[] allFeatureName = {
            "Unnamed: 0", "trans_date_trans_time", "cc_num", "merchant", "category",
            "amt", "first", "last", "gender", "street", "city", "state", "zip", "lat",
            "long", "city_pop", "job", "dob", "trans_num", "unix_time", "merch_lat",
            "merch_long", "categ_amt_mean", "categ_mat_stdev", "is_fraud",
            "category_food_dining", "category_gas_transport",
            "category_grocery_net", "category_grocery_pos",
            "category_health_fitness", "category_home", "category_kids_pets",
            "category_misc_net", "category_misc_pos", "category_personal_care",
            "category_shopping_net", "category_shopping_pos", "category_travel",
            "gender_M", "age", "dist"};
    public static HashMap<String, Integer> featureToIndex = new HashMap<>();

    static {
        // we don"t need to the first index
        for (int i = 1; i < allFeatureName.length; i++) {
            featureToIndex.put(allFeatureName[i], i);
        }
    }

    private List<String> attribute;
    private final String ccNum;
    private final String transNum;

    public Transaction(List<String> stockArr) {
        this.attribute = stockArr;
        ccNum = getFeature("cc_num");
        transNum = getFeature("trans_num");
    }

    public Transaction() {
        ccNum = "";
        transNum = "";
    }

    public String getCcNum() {
        return ccNum;
    }

    public String getTransNum() {
        return transNum;
    }

    public List<String> getAttribute() {
        return attribute;
    }

    public void setAttribute(List<String> attribute) {
        this.attribute = attribute;
    }

    public String getFeature(String featureName) {
        return attribute.get(featureToIndex.get(featureName));
    }

}
