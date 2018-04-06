package com.treee.mongo;


import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

public class connectMongo {

    public static MongoClient getClient(String ip,int port)
    {
        try
        {
            return new MongoClient(ip,port);
        }
        catch(Exception e)
        {
            System.out.println("获取MongoClient失败，"+e.getMessage());
            return null;
        }

    }

    public static MongoDatabase getDB(MongoClient mongoClient,String dbName) {
        try
        {
            return mongoClient.getDatabase(dbName);
        }
        catch(Exception e)
        {
            System.out.println("获取数据库失败,"+e.getMessage());
            return null;
        }

    }

    public static boolean closeClient(MongoClient mongoClient)
    {
        try
        {
            mongoClient.close();

            return true;
        }
        catch(Exception e)
        {
            System.out.println(e.getMessage());
            return false;
        }

    }

}
