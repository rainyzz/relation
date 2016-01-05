package com.rainyzz.relation.util;

import com.clearspring.analytics.util.Lists;
import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.rainyzz.relation.core.Count;
import scala.Tuple2;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by rainystars on 12/24/2015.
 */
public class Evalution {
    static final String LABEL_PATH = "C:\\Users\\rainystars\\Desktop\\relation\\med-label.txt";
    static final String DATA_PATH = "C:\\Users\\rainystars\\Desktop\\relation\\med-data.txt";
    private static Set<String> set = Sets.newHashSet();

    public static void  main(String[] args){
        readLabelData(LABEL_PATH);
        Map<String,Double> map5 = evaluate(DATA_PATH,5);
        Map<String,Double> map10 =evaluate(DATA_PATH,10);
        Map<String,Double> map15 = evaluate(DATA_PATH,15);
        Map<String,Double> map20 =evaluate(DATA_PATH,20);
        for(String key:map10.keySet()){
            System.out.println(key+"\t"+(map5.get(key)+map10.get(key)+map15.get(key)+map20.get(key))/4);
        }
    }
    public static void readLabelData(String path){
        try {
            FileReader fr = new FileReader(new File(path));
            BufferedReader br = new BufferedReader(fr);
            String line;
            while((line = br.readLine()) != null){
                List<String> all = Splitter.on("\t").splitToList(line);
                String mainWord = all.get(0);
                List<String> words = Splitter.on(",").splitToList(all.get(1));
                for(String word :words){
                    set.add(mainWord+"#"+word);
                }


            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static Map<String,Double> evaluate(String path,int topN){
        List<Tuple2<Tuple2<String,String>,Integer>> data = Lists.newArrayList();
        Map<String,Integer> wordCount = Maps.newHashMap();

        try {
            FileReader fr = new FileReader(new File(path));
            BufferedReader br = new BufferedReader(fr);
            String line;
            while((line = br.readLine()) != null){
                List<String> all = Splitter.on("\t").splitToList(line);
                String mainWord = all.get(0);
                String tag = all.get(2);
                List<String> words = Splitter.on(",").splitToList(all.get(1));
                if(topN < words.size()){
                    words = words.subList(0,topN);
                }
                if(wordCount.containsKey(mainWord)){
                    wordCount.put(mainWord,wordCount.get(mainWord)+1);
                }else{
                    wordCount.put(mainWord,1);
                }

                int count = 0;
                for(String word:words){
                    if(set.contains(mainWord+"#"+word)){
                        count++;
                    }
                }
                data.add(new Tuple2<>(new Tuple2<>(mainWord,tag),count));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        Count<String> result = new Count<>();
        int finalCount = 0;
        for(String key:wordCount.keySet()){
            if(wordCount.get(key)==6){
                finalCount++;
            }
        }
        for(Tuple2<Tuple2<String,String>,Integer> tp : data){
            String main = tp._1()._1();
            String tag = tp._1()._2();
            Integer count = tp._2();
            if(wordCount.get(main)!=6){
                continue;
            }else{
                result.increase(tag,count * 1.0 / topN);
            }
        }
        Map<String,Double> map = new HashMap<>();
        for(Map.Entry<String,Double> e :result.entrySet()){
            map.put(e.getKey(),e.getValue()/finalCount);
            //System.out.println(e.getKey()+"#"+e.getValue()/finalCount);
        }
        return  map;
    }
}
