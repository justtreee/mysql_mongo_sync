package com.treee.mongo;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;


public class sync2mongo {
    public static void sync(String MSG){
        String ip="localhost";
        int port=27017;
        String dbName="test";
        MongoClient client=connectMongo.getClient(ip, port);
        if(client!=null)
        {
            MongoDatabase db=connectMongo.getDB(client, dbName);
            if(db!=null)
            {
                System.out.println("connected!");
                System.out.println(MSG);
            }
        }
    }
}
