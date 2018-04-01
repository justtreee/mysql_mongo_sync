package com.treee;

import java.net.InetSocketAddress;
import java.util.stream.Collectors;
import java.util.ArrayList;
import java.util.List;
import java.util.Map; 
import java.util.HashMap;
import java.util.Properties;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.fastjson.JSON;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeoutException;

//读取 Properties
import java.util.Properties;
import java.io.InputStream;   
import java.io.IOException;
import java.io.FileInputStream;

//写入文件
import java.io.FileWriter;

//kafka
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import com.treee.kafka.kafkaProducer; //我的生产者

public class SimpleClient {
    public static String data_dir = "data"; //数据保存路径
    public static void main(String[] args) throws Exception {
        CanalConnector connector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("127.0.0.1", 11111), "example", "", "");

        connector.connect();
        connector.subscribe(".*\\..*");
        connector.rollback();

        while (true) {
            Message message = connector.getWithoutAck(100);
            long batchId = message.getId();
            if (batchId == -1 || message.getEntries().isEmpty()) {
//                System.out.println("sleep");
                Thread.sleep(3000);
                continue;
            }
            printEntries(message.getEntries());
            connector.ack(batchId);
        }
    }

    private static void printEntries(List<Entry> entries) throws Exception {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String timeStr = df.format(new Date());
    	
        ArrayList<String> dataArray = new ArrayList<String> ();
        
        for (Entry entry : entries) {
            if (entry.getEntryType() != EntryType.ROWDATA || entry.getEntryType() == EntryType.TRANSACTIONEND) {
                continue;
            }

	    RowChange rowChage = null;  
            try {  
                rowChage = RowChange.parseFrom(entry.getStoreValue());  
            } catch (Exception e) {  
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),e);  
            }  
            //单条 binlog sql
            EventType eventType = rowChage.getEventType();
            /**
            System.out.println(String.format("================> binlog[%s:%s] , name[%s,%s] , eventType : %s",  
                                             entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),  
                                             entry.getHeader().getSchemaName(), entry.getHeader().getTableName(),  
                                             eventType)); **/
            
            String header_str = "{\"binlog\":\"" + entry.getHeader().getLogfileName()+ ":" + entry.getHeader().getLogfileOffset() + "\"," +
            					"\"db\":\"" + entry.getHeader().getSchemaName() + "\"," +
            					"\"table\":\"" + entry.getHeader().getTableName() + "\",";
            //受影响 数据行
            for (RowData rowData : rowChage.getRowDatasList()) {
            	String row_str = "\"eventType\":\"" + eventType +"\",";
            	String before = "\"\"";
            	String after = "\"\"";
            	
            	//获取字段值 
                if (eventType == EventType.DELETE) {  
                	after = printColumn(rowData.getBeforeColumnsList());  
                } else if (eventType == EventType.INSERT) {  
                	after = printColumn(rowData.getAfterColumnsList());  
                } else {  //update
                    //System.out.println("-------> before");  
                    before = printColumn(rowData.getBeforeColumnsList());  
                    //System.out.println("-------> after");  
                    after = printColumn(rowData.getAfterColumnsList());  
                }
                
                String row_data = header_str + row_str + "\"before\":" +before + ",\"after\":" + after + ",\"time\":\"" + timeStr +"\"}";
                dataArray.add(row_data);   
                save_data_logs(row_data);
                kafkaProducer.send(row_data);
		//push_kafka(row_data);
		System.out.println(row_data);
            }  
            
            
            
            
            //========================================
            /* RowChange rowChange = RowChange.parseFrom(entry.getStoreValue());
            for (RowData rowData : rowChange.getRowDatasList()) {
		System.out.print("{");
		switch (rowChange.getEventType()) {
                case INSERT:
                    System.out.print("\"eventType\" : \"INSERT\",");
                case UPDATE:
                    System.out.print("\"eventType\" : \"UPDATE\",");
		    printColumns(rowData.getAfterColumnsList());

                    if ("retl_buffer".equals(entry.getHeader().getTableName())) {
                        String tableName = rowData.getAfterColumns(1).getValue();
                        String pkValue = rowData.getAfterColumns(2).getValue();
                        System.out.println("SELECT * FROM " + tableName + " WHERE id = " + pkValue);
                    }
                    break;

                case DELETE:
                    System.out.print("\"eventType\" : \"DELETE\",");
                    printColumns(rowData.getBeforeColumnsList());
                    break;

                default:
                    break;
                } */
            
        }
    }

    private static String printColumn(List<Column> columns) {
	//String column_str = "";
    	Map<String, String> column_map = new HashMap<String, String>();
        for (Column column : columns) {
        	String column_name = column.getName();
        	String column_value = column.getValue();
			
			/**
			String a = "";
			String b = "";
			String c = "";
			try {
				column_value = new String(column_value.getBytes(),"UTF-8");
				a=new String(column.getValue().getBytes("Shift-JIS"),"GBK");
				b= new String(column.getValue().getBytes("Shift_JIS"), "GB2312");
				c= new String(column.getValue().getBytes("ISO-8859-1"), "UTF-8");
				
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				//e.printStackTrace();
				//column_value = column.getValue();
			}
			System.out.println("column_value:" + column_value + " : "+getEncoding(column_value)+
					", a:"+ a+" : " +getEncoding(a) +
					", b:"+ b+" : " +getEncoding(b) +
					", c:"+ c+" : " +getEncoding(c));
			**/
        	column_map.put(column_name, column_value);
            //System.out.println(column.getName() + " : " + column.getValue() + " update=" + column.getUpdated());  
        }
        return JSON.toJSONString(column_map);

//        String line = "\"" + columns.stream()
//                .map(column -> column.getName() + "\":\"" + column.getValue())
//                .collect(Collectors.joining("\",\""));
//        System.out.println(line + "\"}");
    }


    //save data file
    private static void save_data_logs(String row_data){
    	String ts = "yyyyMMdd";	
        SimpleDateFormat df2 = new SimpleDateFormat(ts);
        String timeStr2 = df2.format(new Date());
    	String filename = data_dir + "/binlog_" + timeStr2 + ".log";
    	
    	FileWriter writer;
        try {
            writer = new FileWriter(filename, true);
            writer.write(row_data + "\r\n");
            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("write file error!");
        }
    }
    private static void push_kafka(String msg){
	String TOPIC = "test"; //kafka创建的topic
	//String CONTENT = "This is a single message"; //要发送的内容
	String BROKER_LIST = "127.0.0.1:9092,127.0.0.1:9092,127.0.0.1:9092"; //broker的地址和端口
	String SERIALIZER_CLASS = "kafka.serializer.StringEncoder"; // 序列化类
	Properties props = new Properties();
	props.put("serializer.class", SERIALIZER_CLASS);
	props.put("metadata.broker.list", BROKER_LIST);
	ProducerConfig config = new ProducerConfig(props);
	Producer<String, String> producer = new Producer<String, String>(config);
	
        System.out.println("=========== send start =======");
	//Send messages.
	List<KeyedMessage<String,String>> messages =
		new ArrayList<KeyedMessage<String, String>>();
	messages.add(new KeyedMessage<String, String>
		(TOPIC, "push_kafka_test"));
        System.out.println("=========== send success =======");
    }
}
