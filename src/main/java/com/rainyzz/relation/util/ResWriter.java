package com.rainyzz.relation.util;

import com.google.common.base.Joiner;
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
        //writeToSolr();
        readDirShort(SPARK_FILE,"lar-med");
        readDirShort(SPARK_UNFILTER_FILE,"lar-un-med");
        //readFile(PRO_FILE,"pro");
        //readFile(PRO_UNFILTER_FILE,"pro-un");
        //readDirShort(PRO_DIR,"pro");
        //readDirShort(PRO_UNFILTER_DIR,"pro-un");
    }

    private static String SPARK_FILE  ="D:\\lar-med-filter";
    private static String SPARK_UNFILTER_FILE  = "D:\\lar-med-unfilter";
    private static String PRO_DIR  = "D:\\med-rt\\pro-filter";
    private static String PRO_UNFILTER_DIR  = "D:\\med-rt\\pro-unfilter";

    private static String INPUT_DIR = "D:\\final-geo-ouput-final";


    /*private static String INPUT_DIR = "D:\\毕业设计测试数据\\结果数据\\geo-all\\";
    private static String SPARK_FILE  ="D:\\实验结果\\word-agriculture";
    private static String SPARK_UNFILTER_FILE  = "D:\\实验结果\\word-agriculture-unfilter";
    private static String PRO_FILE  = "D:\\实验结果\\output-agriculture.txt";
    private static String PRO_UNFILTER_FILE  = "D:\\实验结果\\output-agriculture-unfilterd.txt";
    private static String PRO_DIR  = "D:\\实验结果\\word-agriculture-pro2";
    private static String PRO_UNFILTER_DIR  = "D:\\实验结果\\word-agriculture-pro2-unfilter";*/

    public static void writeSparkResultToSolr(){

    }

    public static void readFile(String path,String typeId){
        SolrClient solr = new HttpSolrClient("http://localhost:8983/solr/compare-med");
        long count = 0;
        try {
            FileReader fr = new FileReader(new File(path));
            BufferedReader br = new BufferedReader(fr);
            String line;

            count++;
            while ((line = br.readLine()) != null) {

                SolrInputDocument solrDoc = new SolrInputDocument();
                int firstComma = line.indexOf(':');
                if(firstComma == -1){
                    continue;
                }
                String mainWord = line.substring(0, firstComma);
                String othersWord = line.substring(firstComma + 3,line.length()-1);
                if(othersWord.length() < 5){
                    continue;
                }
                List<String> records = Splitter.on(", ").splitToList(othersWord);
                System.out.println(mainWord+"#"+records);
                List<String> processedRecords = new ArrayList<>();
                for(String record:records){
                    List<String> items = Splitter.on(":").splitToList(record);
                    if(items.size() != 2){
                        continue;
                    }
                    String w = items.get(0);

                    String value = null;
                    try {
                        value = String.format("%.9f", Double.parseDouble(items.get(1)));
                    } catch (NumberFormatException e) {
                        continue;
                    }
                    processedRecords.add(w+","+value);
                }
                count++;
                solrDoc.addField("id",typeId+"#"+count);
                solrDoc.addField("mainWord",mainWord);
                solrDoc.addField("words_m",processedRecords);
                solrDoc.addField("type",typeId);
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
    public static void readDirShort(String path,String typeId){
        List<File> fileList = Util.getDirList(path);
        long count = 0;
        SolrClient solr = new HttpSolrClient("http://localhost:8983/solr/compare-lar");
        for(File file:fileList) {
            String dirName = file.getName();
            if (!dirName.contains("part") || dirName.contains("crc")) {
                continue;
            }
            try {
                FileReader fr = new FileReader(file);
                BufferedReader br = new BufferedReader(fr);
                String line;

                count++;
                while ((line = br.readLine()) != null) {

                    SolrInputDocument solrDoc = new SolrInputDocument();
                    int firstComma = line.indexOf(',');
                    if(firstComma == -1){
                        continue;
                    }
                    if(line.length() <5){
                        continue;
                    }
                    System.out.println(line);
                    String othersWord = null;
                    String mainWord = null;
                    try {
                        mainWord = line.substring(1, firstComma);
                        othersWord = line.substring(firstComma,line.length());
                    } catch (Exception e) {
                        continue;
                    }
                    if(othersWord.length() < 5){
                        continue;
                    }
                    List<String> records = Splitter.on("), (").splitToList(othersWord.substring(3,othersWord.length()-3));
                    System.out.println(mainWord+"#"+records);
                    List<String> processedRecords = new ArrayList<>();
                    for(String record:records){
                        List<String> items = Splitter.on(",").splitToList(record);
                        String w = items.get(0);

                        String value = null;
                        try {
                            value = String.format("%.9f", Double.parseDouble(items.get(1)));
                        } catch (NumberFormatException e) {
                            continue;
                        }
                        processedRecords.add(w+","+value);
                    }
                    count++;
                    solrDoc.addField("id",typeId+"#"+count);
                    solrDoc.addField("mainWord",mainWord);
                    solrDoc.addField("words_m",processedRecords);
                    solrDoc.addField("type",typeId);
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

    public static void readDir(String path,String typeId){
        List<File> fileList = Util.getDirList(path);
        long count = 0;
        SolrClient solr = new HttpSolrClient("http://localhost:8983/solr/compare-med");
        for(File file:fileList) {
            String dirName = file.getName();
            if (!dirName.contains("part") && !dirName.contains("crc")) {
                continue;
            }
            try {
                FileReader fr = new FileReader(file);
                BufferedReader br = new BufferedReader(fr);
                String line;

                count++;
                while ((line = br.readLine()) != null) {

                    SolrInputDocument solrDoc = new SolrInputDocument();
                    int firstComma = line.indexOf(',');
                    if(firstComma == -1){
                        continue;
                    }
                    String mainWord = null;
                    try {
                        mainWord = line.substring(1, firstComma);
                    } catch (Exception e) {
                        continue;
                    }
                    String othersWord = line.substring(firstComma,line.length());
                    if(othersWord.length() < 5){
                        continue;
                    }
                    List<String> records = Splitter.on("), (").splitToList(othersWord.substring(3,othersWord.length()-3));
                    System.out.println(mainWord+"#"+records);
                    List<String> processedRecords = new ArrayList<>();
                    for(String record:records){
                        List<String> items = Splitter.on(",").splitToList(record);
                        String w = items.get(0);

                        String value = null;
                        try {
                            value = String.format("%.9f", Double.parseDouble(items.get(5)));
                        } catch (NumberFormatException e) {
                            continue;
                        }
                        processedRecords.add(w+","+value);
                    }
                    count++;
                    solrDoc.addField("id",typeId+"#"+count);
                    solrDoc.addField("mainWord",mainWord);
                    solrDoc.addField("words_m",processedRecords);
                    solrDoc.addField("type",typeId);
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

    public static void writeToSolr(){
        List<File> dirList = Util.getDirList(INPUT_DIR);
        SolrClient solr = new HttpSolrClient("http://localhost:8983/solr/result2");
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
                        if(firstComma == -1){
                            continue;
                        }
                        String mainWord = null;
                        if(firstComma <= 1){
                            continue;
                        }
                        try {
                            mainWord = line.substring(1, firstComma);
                        } catch (Exception e) {
                            continue;
                        }
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
                            String value = null;
                            try {
                                value = String.format("%.6f", Double.parseDouble(items.get(5))*100);
                            } catch (NumberFormatException e) {
                                continue;
                            }
                            processedRecords.add(w+","+value);
                        }
                        count++;
                        if(processedRecords.size() > 50){
                            processedRecords = processedRecords.subList(0,50);
                        }
                        solrDoc.addField("id", count);
                        solrDoc.addField("mainWord",mainWord);
                        solrDoc.addField("mainWordCount",mainWordCount+"."+year+"9");
                        solrDoc.addField("words", Joiner.on("##").join(processedRecords));
                        //solrDoc.addField("words_m",processedRecords);
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
                List<Map.Entry> list = map.sort(100);
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
                List<Map.Entry> list = map.sort(100);

                fw.write(WordMap.get(index) + ": " + trans(index,list,wordTotalCount,wordCount, wordCoCount) +"\n");
            }
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
