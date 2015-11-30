package org.apache.flume.sink.hdfs;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
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
public class TestGson {
    public static void main(String[] args) throws JSONException {
        //log_time:long,hos_id:string,log_type:string,user_mac:string,supp_id:string
        Map<String,String> tableFieldsConf=(Map<String,String>)new ListOrderedMap();
        /**
         suppId:string,logTime:bigint,url:string;userAgent:string
         */

        tableFieldsConf.put("follow","string");
        tableFieldsConf.put("loginVersion","string");
        tableFieldsConf.put("arriveVersion","string");
        tableFieldsConf.put("hosIdarriveVersion","string");
        tableFieldsConf.put("appIdarriveVersion","string");
        tableFieldsConf.put("weixinWifiShopid","string");
        tableFieldsConf.put("weixinWifiBssid","string");
        tableFieldsConf.put("originId","string");
        tableFieldsConf.put("weixinWifiSsid","string");
        tableFieldsConf.put("openId","string");
        tableFieldsConf.put("gwId","string");
        tableFieldsConf.put("suppId","string");
        tableFieldsConf.put("logTime","string");
        tableFieldsConf.put("url","string");
        tableFieldsConf.put("userAgent","string");
        String eventBodyStr="portal\t{\"appId\":\"wxac0f449b34ef6062\",\"arriveVersion\":\"portal_v3_qs\",\"follow\":1,\"gwId\":\"61060300FF0C4781\",\"hosId\":816,\"logId\":34690,\"logTime\":1448855497366,\"loginVersion\":\"v1.1\",\"openId\":\"oMv7YsjCVwe5B3TSNNw26mGSUOxQ\",\"originId\":\"oMv7YsjCVwe5B3TSNNw26mGSUOxQ\",\"suppId\":5,\"url\":\"http://hoswifi.bblink.cn/snappy/pre_arrive.html?p_=v1.1$portal_v3_qs$816$wxac0f449b34ef6062$5$61060300FF0C4781&weixin_wifi_shopId=0&weixin_wifi_bssid=94:b4:0f:bf:3d:42&originid=oMv7YsjCVwe5B3TSNNw26mGSUOxQ&weixin_wifi_ssid=@Hos-WiFi&openId=oMv7YsjCVwe5B3TSNNw26mGSUOxQ\",\"userAgent\":\"Mozilla/5.0 (Linux; U; Android 4.4.2; zh-cn; PE-TL00M Build/HuaweiPE-TL00M) AppleWebKit/533.1 (KHTML, like Gecko)Version/4.0 MQQBrowser/5.4 TBS/025478 Mobile Safari/533.1 MicroMessenger/6.3.7.51_rbb7fa12.660 NetType/WIFI Language/zh_CN\",\"weixinWifiBssid\":\"94:b4:0f:bf:3d:42\",\"weixinWifiShopid\":\"0\",\"weixinWifiSsid\":\"@Hos-WiFi\"}";
        String[] bodyStrArray=eventBodyStr.split("\t");
        String dfrom=bodyStrArray[0];

        JsonElement jelement = new JsonParser().parse(bodyStrArray[1]);
        JsonObject obj= jelement.getAsJsonObject();

        StringBuffer sbf=new StringBuffer(dfrom);
        for(Map.Entry<String,String> entry:tableFieldsConf.entrySet()){
            sbf.append("\t");
            if("string".equals(entry.getValue())){
                System.out.println(entry.getKey());
                if(null==obj.get(entry.getKey())){
                    sbf.append("");
                }else if(obj.get(entry.getKey()).isJsonNull()){
                    sbf.append("");
                }else {
                    sbf.append(obj.get(entry.getKey()).getAsString());
                }
            }else if("bigint".equals(entry.getValue())){
                sbf.append(obj.get(entry.getKey()).getAsBigInteger());
            }
        }


        System.out.println(sbf.toString());
    }
}
