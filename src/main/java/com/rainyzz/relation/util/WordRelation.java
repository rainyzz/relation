package com.rainyzz.relation.util;

import com.alibaba.fastjson.JSON;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import java.io.IOException;
import java.util.*;

/**
 * Created by rainystars on 11/19/2015.
 */

public class WordRelation {
    String[] filterWords = {"进行","采取","达到","减少","增加","使用","实现","改善"};
    public void search(String q){
        SolrClient solr = new HttpSolrClient("http://localhost:8983/solr/result");
        SolrQuery query = new SolrQuery(/*"mainWord:"+*/q);
        Set<String> filterSet = new HashSet<>(Arrays.asList(filterWords));
        query.setRows(500);

        QueryResponse response = null;
        try {
            response = solr.query(query);
        } catch (SolrServerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        Map<String,StringBuffer> optionData = new HashMap<>();
        for(int i = 2000; i <= 2013;i++){
            String year = String.valueOf(i);
            StringBuffer sb = new StringBuffer();
            sb.append(String.format("{'name':'青海','value':1.%s9},",i));
            optionData.put(year,sb);
        }

        SolrDocumentList docs = response.getResults();
        Map<String,String> allData = new HashMap<>();

        for(SolrDocument doc: docs){
            String year = doc.get("year").toString();
            String province = doc.get("province").toString();
            if(province.equals("内蒙")){
                province = "内蒙古";
            }
            String count = doc.get("mainWordCount").toString();
            StringBuffer sb = optionData.get(year);
            if(count.startsWith("0.0")){
                continue;
            }
            sb.append(String.format("{'name':'%s','value':%s},",province,count));

            StringBuffer wordsSb = new StringBuffer();
            List<Object> words = new ArrayList<>(doc.getFieldValues("words_m"));
            if(words.size()> 50){
                words = words.subList(0,50);
            }
            for(Object word:words){
                if(filterSet.contains(word.toString().split(",")[0])){
                    continue;
                }

                wordsSb.append(String.format("{'text':'%s','weight':%s},",
                        word.toString().split(",")[0],word.toString().split(",")[1]));
            }
            if(wordsSb.toString().length() < 5){
                continue;
            }
            allData.put(province+"#"+year,"["+wordsSb.toString().substring(0,wordsSb.toString().length()-1)+"]");

        }
        for(String key:optionData.keySet()){
            StringBuffer sb = optionData.get(key);
            System.out.println("["+sb.toString()+"]");
        }
       ;
        System.out.println(JSON.toJSON(allData));
    }
    public static void main(String args[]){
        WordRelation wr = new WordRelation();
        wr.search("mainWord:水稻 AND year:2013");
    }
}
