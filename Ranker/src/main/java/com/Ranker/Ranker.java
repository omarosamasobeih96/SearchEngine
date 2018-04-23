package com.Ranker;
import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Ranker {
    class Pair {
        public Integer id;
        public Double priority;
        Pair(Integer pId, Double pKey) {
            id = pId;
            priority = pKey;
        }
    }

    MongoCollection<Document> mLinks;

    Map<Integer, Double> mPriorities = new HashMap<Integer, Double>();
    Map<Integer, Integer> mUrlWordsCnt = new HashMap<Integer, Integer>();

    ArrayList<Document> mWords;

    int mDocumentsCnt;

    public Ranker(int documentsCnt, ArrayList<Document> words, MongoCollection<Document> links) {
        mWords = words;
        mDocumentsCnt = documentsCnt;
        mLinks = links;
    }


    ArrayList<Integer> Sort() {
        ArrayList<Integer> ret = new ArrayList<Integer>();
        ArrayList<Pair> urls = new ArrayList<Pair>();
        for(Map.Entry<Integer, Double> url : mPriorities.entrySet()) {
            Pair tmp = new Pair(url.getKey(), url.getValue());
            urls.add(tmp);
        }
        for(int i = 0; i < urls.size(); ++i) {
            int j = i, k = (i - 1) / 2;
            while(j > 0) {
                Pair a = urls.get(k);
                Pair b = urls.get(j);
                if(a.priority < b.priority) {
                    Pair t = new Pair(a.id, a.priority);
                    urls.set(k, new Pair(b.id, b.priority));
                    urls.set(j, new Pair(t.id, t.priority));
                    j = k;
                    k = (k - 1) / 2;
                }
                else break;
            }
        }
        int n = urls.size();
        while (n > 0) {
            n = n - 1;
            Pair a = urls.get(0);
            Pair b = urls.get(n);
            Pair t = new Pair(a.id, a.priority);
            urls.set(0, new Pair(b.id, b.priority));
            urls.set(n, new Pair(t.id, t.priority));
            int j, i = 0;
            while((j = i * 2 + 1) < n) {
                if(j + 1 < n && urls.get(j).priority < urls.get(j + 1).priority)j = j + 1;
                if(urls.get(i).priority < urls.get(j).priority){
                    a = urls.get(i);
                    b = urls.get(j);
                    t = new Pair(a.id, a.priority);
                    urls.set(i, new Pair(b.id, b.priority));
                    urls.set(j, new Pair(t.id, t.priority));
                    i = j;
                }
                else break;
            }
        }
        for(int i = urls.size() - 1; i >= 0; --i)ret.add(urls.get(i).id);
        return  ret;
    }

    ArrayList<Integer> Rank() {
        for (Document word : mWords) {
            Document details = (Document)word.get("details");
            Set<String> urls = details.keySet();
            for(String urlID : urls) {
                int key = Integer.parseInt(urlID);
                mPriorities.put(key, 0.0);
                if(!mUrlWordsCnt.containsKey(key)) {
                    int cnt = mLinks.find(new BasicDBObject("id", key)).first().getInteger("numOfWords");
                    mUrlWordsCnt.put(key, cnt);
                }
            }
        }
        for (Document word : mWords) {
            Document details = (Document)word.get("details");
            Set<String> urls = details.keySet();
            double IDF = Math.log(1.0 * mDocumentsCnt / urls.size());
            for(String urlID : urls) {
                Document url = (Document) details.get(urlID);
                ArrayList<Integer> tags = (ArrayList<Integer>) url.get("tag");
                int key = Integer.parseInt(urlID);
                double TF = 1.0 * tags.size() / mUrlWordsCnt.get(key);
                if(TF > 0.5)continue;
                double tag = 0.0;
                for(int i = 0; i < tags.size(); ++i) {
                    tag += tags.get(i);
                }
                tag /= tags.size();
                double priority = IDF * tag * TF;
                mPriorities.put(key, mPriorities.get(key) + priority);
            }
        }

        return Sort();
    }
}
