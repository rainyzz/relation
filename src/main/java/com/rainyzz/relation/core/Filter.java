package com.rainyzz.relation.core;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.FieldStatsInfo;
import org.apache.solr.client.solrj.response.PivotField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.util.NamedList;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Created by rainystars on 10/23/2015.
 */
public class Filter {
    public static Set<String> dict = null;
    private static final int TOTAL = 8;
    public static final String BUILD_LIBRARY_PTAH = "C:\\Users\\rainystars\\Desktop\\relation\\final.txt";
    public static final String STOPWORDS_PTAH = "stopwords.txt";
    public static void buildDict(){
        if(dict == null){
            dict = Sets.newHashSetWithExpectedSize(400 * 10000);
        }
        long beginTime = System.currentTimeMillis();

        String fileName = BUILD_LIBRARY_PTAH;
        try {
            FileReader fr = new FileReader(new File(fileName));
            BufferedReader br = new BufferedReader(fr);
            String line;
            int count = 0;
            while ((line = br.readLine()) != null) {
                if(line.trim().length() < 2){
                    continue;
                }
                if(!Strings.isNullOrEmpty(line)){
                    dict.add(line.trim());
                }
                count++;
                if(count % 100000 == 0){
                    System.out.println(count);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        long endTime=System.currentTimeMillis();
        long costTime = (endTime - beginTime);
        System.out.println("Dict built in " + costTime / 1000.0 + "s");
    }

    public static void calcTypeWords(){
        SolrClient solr = new HttpSolrClient("http://localhost:8983/solr/filter");
        SolrQuery query = new SolrQuery("code1:T");
        query.setFacet(true);
        query.set("stats", true);
        query.setRows(0);
        query.set("stats.field", "{!tag=piv1}id");
        query.set("facet.pivot", "{!stats=piv1}code2,text");
        query.setFacetLimit(200);
        QueryResponse response = null;
        try {
            response = solr.query(query);
        } catch (SolrServerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        if(response != null){
            processPivots(response,"code2","text");
        }
    }
    private static Set<String> stopwords = null;
    public static void initStopwords(){
        if(stopwords == null){
            stopwords = Sets.newHashSet();
        }
        String fileName = STOPWORDS_PTAH;
        try {
            FileReader fr = new FileReader(new File(fileName));
            BufferedReader br = new BufferedReader(fr);
            String line;
            while ((line = br.readLine()) != null) {
                stopwords.add(line.trim());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void processPivots(QueryResponse response, String... fields){
        initStopwords();
        NamedList<List<PivotField>> namelist = response.getFacetPivot();
        String pivots = Joiner.on(",").join(fields);
        List<PivotField> list = namelist.get(pivots);
        Map<String,Map<String,Long>> topWords = Maps.newHashMap();
        for(PivotField pf:list){
            Object typeName = pf.getValue();
            List<PivotField> subPivots = pf.getPivot();
            Map<String,Long> singleTop = Maps.newHashMap();
            //System.out.println(typeName);
            if(subPivots != null){
                for(PivotField spf:subPivots){
                    Map<String, FieldStatsInfo> sstatsInfo = spf.getFieldStatsInfo();
                    FieldStatsInfo sstats = sstatsInfo.get("id");
                    String word = spf.getValue().toString();
                    long count = sstats.getCount();
                    if(count < 10 || spf.getValue() == null || stopwords.contains(word) || word.length() < 2){
                        continue;
                    }
                    singleTop.put(word.toString(),count);
                    System.out.println(typeName+"-"+word+":"+sstats.getCount());
                }
            }
            topWords.put(typeName.toString(),singleTop);
        }
        Map<String,Long> finalTop = Maps.newHashMap();
        for(String type:topWords.keySet()){
            Map<String,Long> top = topWords.get(type);
            for (String word:top.keySet()){
                if(finalTop.containsKey(word)){
                    finalTop.put(word,finalTop.get(word)+1);
                }else{
                    finalTop.put(word,1L);
                }
            }
        }
        List<Map.Entry<String,Long>> finalTopList = new ArrayList<Map.Entry<String,Long>>(finalTop.entrySet());
        Collections.sort(finalTopList, new Comparator() {
            public int compare(Object o1, Object o2) {
                return 0 - ((Comparable) ((Map.Entry) (o1)).getValue())
                        .compareTo(((Map.Entry) (o2)).getValue());
            }
        });

        for(Map.Entry<String,Long> entry:finalTopList){
            long v = entry.getValue();
            if(v == 1){
                continue;
            }
            System.out.println(entry.getKey()+"-"+ v * 1.0 / TOTAL );

        }
        //System.out.println(finalTopList);
    }

    public static void main(String[] args){
        calcTypeWords();
    }

}
