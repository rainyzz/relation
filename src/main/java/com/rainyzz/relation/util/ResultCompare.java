package com.rainyzz.relation.util;

import com.google.common.base.Splitter;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by rainystars on 11/30/2015.
 */
public class ResultCompare {

    static String[] keywords = {"基因","氮肥","病毒","基因","小麦","水稻","免疫","肥料","抗体","克隆","大米","繁殖","品种","细胞","植物","蛋白","细胞","细菌","昆虫","饲料","栽培","培育","种植","作物","蔬菜","营养","土地","土壤","安全","播种"};
//static String[] keywords = {"基因","肝脏","病毒","基因","细菌","心脏","肝病","患者","血管","临床","血管","动脉","静脉","细胞","粘膜","皮肤","神经","烧伤","关节","疼痛","注射","药品","蔬菜","感染","营养","土壤","输液","注射"};

    public static String DATA_FILE = "D://compare-arg-lar-data.txt";
    public static String TAG_FILE = "D://compare-arg-lar-tag.txt";

    public static void  main(String[] args){
        search();
    }

    public static void search(){
        SolrClient solr = new HttpSolrClient("http://localhost:8983/solr/compare-lar");

        BufferedWriter outData = null;
        BufferedWriter outTag = null;
        try {
            outData = new BufferedWriter(new FileWriter(new File(DATA_FILE)));
            outTag = new BufferedWriter(new FileWriter(new File(TAG_FILE)));
        } catch (IOException e) {
            e.printStackTrace();

        }
        for(String keyword:keywords){
            SolrQuery query = new SolrQuery("mainWord:"+keyword);

            QueryResponse response = null;
            try {
                response = solr.query(query);

            } catch (SolrServerException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            SolrDocumentList list = response.getResults();
            /*if(list.size()!=4){
                continue;
            }*/
            int count = 1;
            try {
                for(SolrDocument doc:list){

                    System.out.println(keyword+"#"+count+"#"+doc.get("type"));
                    outTag.write(keyword+"#"+count+"#"+doc.get("type")+"\n");

                    List<Object> wordList = new ArrayList<>(doc.getFieldValues("words_m"));
                    List<String> result = new ArrayList<>();
                    for(Object wordOb:wordList){
                        String word = Splitter.on(",").splitToList(wordOb.toString()).get(0);
                        result.add(word);
                    }
                    if(result.size() > 20){
                        result = result.subList(0,20);
                    }
                    System.out.println(keyword+"#"+count+"#"+result);
                    outData.write(keyword+"#"+count+"#"+result+"\n");
                    count++;
                }
                outTag.flush();
                outData.flush();

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            outTag.flush();
            outData.flush();
            outTag.close();
            outData.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
