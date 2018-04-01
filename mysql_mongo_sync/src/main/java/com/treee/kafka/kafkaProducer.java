package com.treee.kafka;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class kafkaProducer {
    private static final String TOPIC = "test"; //kafka创建的topic
    private static final String CONTENT = "This is a single message"; //要发送的内容
    private static final String BROKER_LIST = "127.0.0.1:9092,127.0.0.1:9092,127.0.0.1:9092"; //broker的地址和端口
    private static final String SERIALIZER_CLASS = "kafka.serializer.StringEncoder"; // 序列化类

    public static void send(String MSG){
        Properties props = new Properties();  
        props.put("serializer.class", SERIALIZER_CLASS);
        props.put("metadata.broker.list", BROKER_LIST);


        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);

        //Send one message.
        KeyedMessage<String, String> message =
                new KeyedMessage<String, String>(TOPIC, MSG);
        producer.send(message);
/*
        //Send multiple messages.
        List<KeyedMessage<String,String>> messages =
                new ArrayList<KeyedMessage<String, String>>();
        for (int i = 0; i < 5; i++) {
            messages.add(new KeyedMessage<String, String>
                    (TOPIC, "Multiple message at a time. " + i));
        }
        producer.send(messages);
*/
    }


}
