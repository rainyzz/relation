package com.rainyzz.relation.util;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.rainyzz.relation.core.ConCalc;
import com.rainyzz.relation.core.Count;
import com.rainyzz.relation.core.WordMap;
import org.apache.log4j.net.SyslogAppender;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;

import java.io.*;
import java.util.*;

/**
 * Created by rainystars on 10/23/2015.
 */
public class ResWriter {
    public static void main(String[] args){
        writeToSolr();
    }

    private static String INPUT_DIR = "D:\\毕业设计测试数据\\结果数据\\geo-all\\";
    public static void writeToSolr(){
        List<File> dirList = Util.getDirList(INPUT_DIR);
        SolrClient solr = new HttpSolrClient("http://localhost:8983/solr/result");
        long count = 0;
        for(File dir:dirList){
            String dirName = dir.getName();
            if(!dirName.contains("#")){
                continue;
            }
            System.out.println(dirName);
            String province = dirName.split("#")[0];
            String year = dirName.split("#")[1];
            List<File> resultFiles = Util.getListFiles(dir.getAbsolutePath());
            for(File resultFile : resultFiles){
                String fileName = resultFile.getName();
                if(!fileName.startsWith("part")){
                    continue;
                }
                try {
                    FileReader fr = new FileReader(resultFile);
                    BufferedReader br = new BufferedReader(fr);
                    String line;

                    while ((line = br.readLine()) != null) {

                        SolrInputDocument solrDoc = new SolrInputDocument();
                        int firstComma = line.indexOf(',');
                        String mainWord = line.substring(1, firstComma);
                        String othersWord = line.substring(firstComma,line.length());
                        if(othersWord.length() < 5){
                            continue;
                        }
                        List<String> records = Splitter.on("), (").splitToList(othersWord.substring(3,othersWord.length()-3));
                        System.out.println(mainWord+"#"+records);
                        List<String> processedRecords = new ArrayList<>();
                        String mainWordCount = "0";
                        for(String record:records){
                            List<String> items = Splitter.on(",").splitToList(record);
                            String w = items.get(0);
                            mainWordCount = items.get(3);
                            String value = String.format("%.9f", Double.parseDouble(items.get(5)));
                            processedRecords.add(w+","+value);
                        }
                        count++;
                        solrDoc.addField("id", count);
                        solrDoc.addField("mainWord",mainWord);
                        solrDoc.addField("mainWordCount",mainWordCount+"."+year+"9");
                        solrDoc.addField("words_m",processedRecords);
                        solrDoc.addField("year",year);
                        solrDoc.addField("province",province);
                        solr.add(solrDoc);
                        if(count % 2000 == 0){
                            solr.commit();
                        }
                    }
                    solr.commit();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        //System.out.println(dirList);
        /*SolrClient solr = new HttpSolrClient("http://localhost:8983/solr/result");
        int count = 0;
        for(String key:frenquecy.keySet()){
            SolrInputDocument solrDoc = new SolrInputDocument();
            solrDoc.addField("id", "NEW");
            solrDoc.addField("word_s", key);

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
        }*/
    }

    private static List<String> trans(List<Map.Entry> list){
        List<String> res = Lists.newArrayList();
        for(Map.Entry<Integer,Double> entry:list){
            res.add(WordMap.get(entry.getKey()) + ":" + entry.getValue());
        }
        return res;
    }
    private static List<String> trans(int mainWord,List<Map.Entry> list,Count wordTotalCount, Count wordCount, Map<Integer,Count> wordCoCount){
        List<String> res = Lists.newArrayList();
        for(Map.Entry<Integer,Double> entry:list){
            int index = entry.getKey();
            res.add(WordMap.get(index) +","+ wordCount.get(mainWord)+":"+wordTotalCount.get(mainWord) +":"
                    +wordCoCount.get(mainWord).get(index)+ ":" + ":" + entry.getValue());
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
    public static void writeResult(Map<Integer,Count> frenquecy,Count wordTotalCount, Count wordCount, Map<Integer,Count> wordCoCount,String filename){

        try {
            FileWriter fw = new FileWriter(new File(filename));
            for(Integer index:frenquecy.keySet()){

                Count map = frenquecy.get(index);
                List<Map.Entry> list = map.sort();

                fw.write(WordMap.get(index) + ": " + trans(index,list,wordTotalCount,wordCount, wordCoCount) +"\n");
            }
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
