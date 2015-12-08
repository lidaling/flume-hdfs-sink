package org.apache.flume.sink.customhdfs;

import org.apache.commons.collections.map.ListOrderedMap;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Map;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
public class TestJson {
    public static void main(String[] args) throws JSONException {
        //log_time:long,hos_id:string,log_type:string,user_mac:string,supp_id:string
        Map<String,String> tableFieldsConf=(Map<String,String>)new ListOrderedMap();

        tableFieldsConf.put("log_typed","string");
        tableFieldsConf.put("supp_id","string");
        tableFieldsConf.put("hos_id","string");
        tableFieldsConf.put("log_time","string");
        tableFieldsConf.put("user_mac","string");
        String eventBodyStr="portal\t{\"log_time\":1111,\"hos_id\":222,\"log_type\":\"4\",\"user_mac\":\"dd:dfsfsdfsdfs\",\"supp_id\":\"3\"}";
        String[] bodyStrArray=eventBodyStr.split("\t");
        String dfrom=bodyStrArray[0];
        JSONObject obj = new JSONObject(bodyStrArray[1]);

        StringBuffer sbf=new StringBuffer(dfrom);
        for(Map.Entry<String,String> entry:tableFieldsConf.entrySet()){
            sbf.append("\t");
            sbf.append(obj.get(entry.getKey()));
        }


        System.out.println(sbf.toString());
    }
}
