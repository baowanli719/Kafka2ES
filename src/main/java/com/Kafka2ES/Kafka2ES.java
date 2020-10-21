package com.Kafka2ES;

import com.Kafka2ES.dao.ConfigurationManager;
import com.Kafka2ES.dao.Constants;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.text.SimpleDateFormat;
import java.util.*;

public class Kafka2ES {
    public static void main(String[] args) {

        System.setProperty("java.security.auth.login.config", ConfigurationManager.getProperty(Constants.SECURITY_AUTH_LOGIN_CONFIG));
        System.setProperty("java.security.krb5.conf", ConfigurationManager.getProperty(Constants.SECURITY_KRB5_CONFIG));

        Properties properties = new Properties();
        //properties.put("bootstrap.servers", ConfigurationManager.getProperty(Constants.KAFKA_BOOTSTRAP_SERVERS));

        properties.put("bootstrap.servers", ConfigurationManager.getProperty(Constants.KAFKA_BOOTSTRAP_SERVERS));
        properties.put("group.id", "Kafka2ES-Realtime-BWL");
        properties.put("enable.auto.commit", "false");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("auto.offset.reset", "latest");
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //kerberos安全认证
        properties.put("security.protocol", "SASL_PLAINTEXT");
        properties.put("sasl.mechanism", "GSSAPI");
        properties.put("sasl.kerberos.service.name", "kafka");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        String topics = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS);
        String[] topicArr = topics.split(",");

        Collection<String> topicSet = Arrays.asList(topicArr);

        kafkaConsumer.subscribe(topicSet);

        /*while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(5000);
            //System.out.println("here");
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, value = %s", record.offset(), record.value());
                System.out.println();
            }


        }*/

        String[] tablename_rule1 = {"STOCKREAL", "CRDTSTOCKREAL", "SECUMREAL"};
        String[] tablename_rule2 = {"FUNDJOUR", ""};
        String[] tablename_rule3 = {"FUNDACCOUNTJOUR"};
        String[] tablename_rule4 = {"BANKTRANSFER"};
        String[] entrust_bs_rule = {"1", "2"};//委托种类
        String[] exchange_type_rule = {"1", "2"};//交易类别
        String[] branch_no_rule = {"8888", "9800", "9900"};//自营机构
        String[] real_status_rule = {"0", "4"};//处理标志
        String[] real_type_rule1 = {"0"};//成交类型
        String[] real_type_rule2 = {"6", "7", "8", "9"};//成交类型
        String[] stock_type_rule = {"0", "1", "d", "c", "h", "e", "g", "D", "L", "6", "T", "p", "q"};//证券类别
        String[] operType_rule = {"I"};
        String[] operType_rule2 = {"I", "U"};
        //operType  D：delete;I:insert;U:update:DT:truncate;
        String[] trans_type_rule = {"01", "02"};//转账类型
        String[] money_type_rule = {"0", "1"};//货币代码
        String[] bktrans_status_rule = {"2"};//转账状
        String[] asset_prop_rule = {"0"};//账户属
        String[] business_flag_rule = {"2041", "2042", "2141", "2142"};//业务品种

        try {
            while (true) {

                ConsumerRecords<String, String> records = kafkaConsumer.poll(3000);
                List<Map<String, Object>> list_stockreal = new ArrayList<Map<String, Object>>();
                List<Date> dates = new ArrayList<Date>();

                for (ConsumerRecord<String, String> line : records) {
                    //System.out.println(line.timestamp() + "," + line.topic() + "," + line.partition() + "," + line.offset() + " " + line.key() + "," + line.value());


                    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    String newline = line.value().replace("\n|\r", "");
                    boolean flag = false;
                    flag = MyUtils.isJson(newline);
                    JSONObject res1 = null;
                    String tablename = null;
                    String timeload = null;
                    String operType = null;

                    if (flag) {
                        res1 = JSON.parseObject(newline);
                        tablename = res1.getString("Tablename");
                        timeload = res1.getString("timeload");
                        operType = res1.getString("operType");

                        try {
                            dates.add(format.parse(timeload));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                        JSONObject res2 = null;

                        if (res1.containsKey("columnInfo")) {
                            res2 = JSON.parseObject(res1.getString("columnInfo"));
                        } else if (res1.containsKey("columns")) {
                            res2 = JSON.parseObject(res1.getString("columns"));
                        } else {
                            flag = false;
                        }


                        if (Arrays.asList(tablename_rule2).contains(tablename)
                                & Arrays.asList(operType_rule2).contains(operType)
                                & flag) {

                            String fund_account = res2.getString("FUND_ACCOUNT");
                            String money_type = res2.getString("MONEY_TYPE");
                            String business_flag = res2.getString("BUSINESS_FLAG");
                            String occur_balance = res2.getString("OCCUR_BALANCE");
                            String rowkey = res2.getString("POSITION_STR");
                            if (Arrays.asList(money_type_rule).contains(money_type)
                                    & Arrays.asList(business_flag_rule).contains(business_flag)) {

                                Map<String, Object> map1 = new HashMap<String, Object>();
                                map1.put("position_str", rowkey);
                                map1.put("money_type", money_type);
                                map1.put("fund_account", fund_account);
                                map1.put("columnValue", occur_balance);
                                map1.put("time_load", timeload);
                                map1.put("index", "hs_asset_fundjour");
                                map1.put("init_date", res2.getString("INIT_DATE"));

                                list_stockreal.add(map1);
                            }

                        } else if (Arrays.asList(tablename_rule1).contains(tablename)
                                & Arrays.asList(operType_rule2).contains(operType)
                                & flag) {
                            String fund_account = res2.getString("FUND_ACCOUNT");
                            String money_type = res2.getString("MONEY_TYPE");
                            String occur_balance = "0";
                            String rowkey = res2.getString("POSITION_STR");

                            Map<String, Object> map1 = new HashMap<String, Object>();
                            map1.put("position_str", rowkey);
                            map1.put("money_type", money_type);
                            map1.put("fund_account", fund_account);
                            map1.put("columnValue", occur_balance);
                            map1.put("time_load", timeload);
                            map1.put("init_date", res2.getString("INIT_DATE"));

                            if (tablename.equals("STOCKREAL")) {
                                map1.put("index", "hs_secu_stockreal");
                            } else if (tablename.equals("CRDTSTOCKREAL")) {
                                map1.put("index", "hs_crdt_crdtstockreal");
                            } else if (tablename.equals("SECUMREAL")) {
                                map1.put("index", "hs_prod_secumreal");
                            }

                            list_stockreal.add(map1);


                        } else {
                            //System.out.println("************************ not match:" + res1.values() + "**************************");
                        }
                    }
                }
                try{
                    //insertMany(hbtable,list);
                    if(list_stockreal.size()>0) {

                        //insertDetail(list_fundjour, dates,"realtime_bank_transfer");
                        //insertDetail2Hbase(list_fundjour,"RL_ARM:BANKTRANSFER_FUNDJOUR");

                        MyEsTools.insertBatch(list_stockreal);
                        //MyUtils.insertDetail2Oracle(list_stockreal, dates,"hs_asset_fundjour");
                        //System.out.println(list_stockreal);

                    } else{
                        System.out.println("没有符合条件的数");
                    }
                }catch (Exception e){
                    e.printStackTrace();
                }

                try {
                    kafkaConsumer.commitSync();
                } catch (CommitFailedException e) {
                    System.out.println("commit failed msg" + e.getMessage());
                }
            }

        } finally{
            kafkaConsumer.close();
        }
    }
}