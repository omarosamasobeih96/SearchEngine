package com.Ranker;


import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;

public class App
{
    private static MongoClient mongoClient;
    private static MongoDatabase database;
    static MongoCollection<org.bson.Document> terms, links;


    public static void main( String[] args ) {
        mongoClient = new MongoClient("localhost" , 27017);
        database = mongoClient.getDatabase("APT");
        terms = database.getCollection("words");
        links = database.getCollection("links");
        FindIterable<Document> words = terms.find();
        int e = 5;
        ArrayList<Document> par = new ArrayList<Document>();
        for(Document doc : words) {
            par.add(doc);
            e = e -1 ;
            if(e == 0)break;
        }
        Ranker ranker = new Ranker(5, par, links);
        ranker.Rank();
    }
}
