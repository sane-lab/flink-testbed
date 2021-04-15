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
            "merch_long", "is_fraud"};
    static HashMap<String, Integer> featureToIndex = new HashMap<>();

    static {
        // we don't need to the first index
        for(int i=1;i<allFeatureName.length;i++){
            featureToIndex.put(allFeatureName[i], i);
        }
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

    private List<String> attribute;

    public Transaction(List<String> stockArr) {
        this.attribute = stockArr;
    }

    public Transaction() {

    }

}
