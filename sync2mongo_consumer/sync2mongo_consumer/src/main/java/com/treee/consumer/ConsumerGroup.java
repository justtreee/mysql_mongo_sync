package com.treee.consumer;
import java.util.Properties;
import java.util.Arrays;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import com.treee.mongo.connectMongo;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;


public class ConsumerGroup {
   public static void main(String[] args) throws Exception{
        /* if(args.length < 2){
            System.out.println("Usage: consumer <topic> <groupname>");
            return;
        } */
        
        String topic = "test";//args[0].toString();
        String group = "grouptest";//args[1].toString();
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", group);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer",          
            "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", 
            "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        
        consumer.subscribe(Arrays.asList(topic));
        //System.out.println("=====================Subscribed to topic " + topic);
        int i = 0;
        // ========= connect mongo =============
        String ip="localhost";
        int port=27017;
        String dbName="test";
        MongoClient client=connectMongo.getClient(ip, port);
        if(client!=null)
        {
            MongoDatabase db=connectMongo.getDB(client, dbName);
            if(db!=null)
            {
                MongoCollection<Document> collection = db.getCollection("test");
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(100);
                    for (ConsumerRecord<String, String> record : records)
                    {   
                        //System.out.printf("====\n=====\n======offset = %d, key = %s, value = %s\n===========\n============\n", 
                        //record.offset(), record.key(), record.value());
                        
                        JSONObject jsonObj = JSON.parseObject(record.value());
                        String eventType = jsonObj.getString("eventType");
                        //System.out.println(eventType);
                        if(eventType.equals("UPDATE")){
                            //System.out.println("UPDAAAAAAAAAAAAATE!!!!");
                            String before = jsonObj.getString("before");
                            String after = jsonObj.getString("after");
        
                            Document bedocument = Document.parse(before);
                            Document afdocument = Document.parse(after);
                            collection.replaceOne(bedocument,afdocument);
                        }
                        else if(eventType.equals("INSERT")){
                            String insrt = jsonObj.getString("after");
                            //System.out.println(insrt);
                            Document document = Document.parse(insrt);
                            collection.insertOne(document);
                        }
                        else if(eventType.equals("DELETE")){
                            //System.out.println("DELEEEEEEEEEEEEEEETE!!!");
                            String del = jsonObj.getString("after");
                            Document document = Document.parse(del);
                            collection.deleteOne(document);
                        }
                        else
                        {
                            System.out.println("ERRRRROR!!!");
                        }
                    }
                }     
            }  
        }
    }
}
