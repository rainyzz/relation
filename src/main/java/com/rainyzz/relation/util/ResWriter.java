package com.rainyzz.relation.util;

import com.google.common.collect.Lists;
import com.rainyzz.relation.core.Count;
import com.rainyzz.relation.core.WordMap;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * Created by rainystars on 10/23/2015.
 */
public class ResWriter {
    public static void writeToSolr(Map<String,Map<String,Double>> frenquecy,int year){
        SolrClient solr = new HttpSolrClient("http://localhost:8983/solr/relation");
        int count = 0;
        for(String key:frenquecy.keySet()){

            Map<String,Double> map = frenquecy.get(key);
            List list = new ArrayList(map.entrySet());
            if(list.size() <= 4){
                continue;
            }
            Collections.sort(list, new Comparator() {
                public int compare(Object o1, Object o2) {
                    return 0 - ((Comparable) ((Map.Entry) (o1)).getValue())
                            .compareTo(((Map.Entry) (o2)).getValue());
                }
            });
            if(list.size() > 50){
                list = list.subList(0,45);
            }
            SolrInputDocument solrDoc = new SolrInputDocument();
            solrDoc.addField("id", "NEW");
            solrDoc.addField("word_s", key);
            solrDoc.addField("relation_s", list.toString());
            solrDoc.addField("year_s", year);


            count++;
            try {
                solr.add(solrDoc);
                if(count %1000 == 0){
                    solr.commit();
                }
            } catch (SolrServerException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static List<String> trans(List<Map.Entry> list){
        List<String> res = Lists.newArrayList();
        for(Map.Entry<Integer,Double> entry:list){
            res.add(WordMap.get(entry.getKey()) + ":" + entry.getValue());
        }
        return res;
    }

    public static void writeResult(Map<Integer,Count> frenquecy,String filename){

        try {
            FileWriter fw = new FileWriter(new File(filename));
            for(Integer index:frenquecy.keySet()){

                Count map = frenquecy.get(index);
                List<Map.Entry> list = map.sort();
                fw.write(WordMap.get(index) + ": " + trans(list) +"\n");
            }
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
