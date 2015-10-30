package com.rainyzz.relation.mapreduce;

import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import com.rainyzz.relation.core.Count;
import com.rainyzz.relation.util.LineReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

/**
 * Created by rainystars on 10/27/2015.
 */
public class WordCount {
    public static class WordCountMapper
            extends Mapper<LongWritable,Text,Text,Text> {
        public static final double E = 2.71828182846;
        public static final double U = 0.1;
        public static final double M = 0.15;
        public static final int HALF_WINDOW = 2;

        //mapper Log
        //private static final Log LOG = LogFactory.getLog(WordCountMapper.class);

        private Text result = new Text();
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String sentence = value.toString();
            Map<String,String> article = LineReader.readRecord(sentence, 0, 0);
            Set<String> words = new HashSet<String>();

            for(String word: Splitter.on(" ").omitEmptyStrings().split(article.get("abs"))){
                words.add(word);
            }
            for(String word:Splitter.on(" ").omitEmptyStrings().split(article.get("title"))){
                words.add(word);
            }
            for(String word:Splitter.on(" ").omitEmptyStrings().split(article.get("keyword"))) {
                words.add(word);
            }

            String[] abstracts = article.get("abs").split(" ");
            String[] titles = article.get("title").split(" ");
            String[] keywords = article.get("keyword").split(" ");

            // 统计单个词出现的文档个数
            for(String w : words){
                word.set(w);
                result.set("1");
                context.write(word,result);
            }

            // 统计词对出现的文档个数
            for(String wordA : words) {
                for (String wordB : words) {
                    if (wordA.equals(wordB)) {
                        continue;
                    }
                    word.set(wordA);
                    result.set("&" +wordB + "@"+1);
                    context.write(word, result);
                }
            }

            // LAR Model
            for(int i = 0; i < abstracts.length; i++){
                String curWord = abstracts[i];

                int windowStart = i - HALF_WINDOW < 0 ? 0 : i - HALF_WINDOW;
                int windowEnd = i + HALF_WINDOW > abstracts.length ? abstracts.length: i + HALF_WINDOW;
                int windowMid = i;

                //对词窗内的词进行统计 coCount

                for(int j = windowStart; j < windowEnd; j++){
                    if(curWord.equals(abstracts[j])){
                        continue;
                    }
                    int distance = Math.abs(i - j);
                    double weight = U * Math.pow(E,- U * distance);
                    result.set("@"+abstracts[j] +"@"+ weight);
                    word.set(curWord);
                    context.write(word,result);
                }

                //对于当前词，其与所有其他字段中词语都有关系。 diffCoCount
                /*int curLen = abs.length();
                int keywordLen = keyword.length();
                for(String key:keywords){

                    if(word.equals(key)){
                        continue;
                    }
                    int keywordIndex = WordMap.set(key);

                    double weight = U * Math.pow(E,- U * (M * curLen / keywordLen));
                    count.increase(keywordIndex,weight);
                }*/
            }
        }
    }
    public static class WordCountReducer
            extends Reducer<Text,Text,Text,Text> {

        private Text result = new Text();
        private Text wordPair = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int wordCount = 0;
            Map<String,Integer> count = Maps.newHashMap();
            Map<String,Double> frequence = Maps.newHashMap();
            for(Text value:values){
                String v = value.toString();
                if(v.startsWith("&")){
                    //CoCount
                    String wordB = v.substring(1,v.length()).split("@")[0];
                    String num = v.substring(1,v.length()).split("@")[1];
                    int n = Integer.valueOf(num);
                    if(count.containsKey(wordB)){
                        count.put(wordB,count.get(wordB)+n);
                    }else{
                        count.put(wordB,n);
                    }
                }else if(v.startsWith("@")) {
                    //frequence
                    String wordB = v.substring(1,v.length()).split("@")[0];
                    String num = v.substring(1,v.length()).split("@")[1];
                    double n = Double.valueOf(num);
                    if(frequence.containsKey(wordB)){
                        frequence.put(wordB,frequence.get(wordB)+n);
                    }else{
                        frequence.put(wordB,n);
                    }

                }else{
                    //Count
                    wordCount += Integer.valueOf(v);
                }
            }
            Map<String,Double> rst = Maps.newHashMap();

            for(String wordB:frequence.keySet()){
                double cocurWeight = frequence.get(wordB);
                int wordcoCount = count.get(wordB);

                double weight = cocurWeight / wordCount
                        * Math.log10(wordcoCount / wordCount + 1);

                if(wordcoCount == 0){
                    continue;
                }

                rst.put(wordB,weight);
                /*result.set("CoCount: "+ wordcoCount + ", Count: "+ wordCount+", Weight:" + weight);
                wordPair.set(key+"&"+wordB);
                context.write(wordPair,result);*/
            }


            List<Map.Entry> list = new ArrayList(rst.entrySet());
            if(list.size() <= 4){
                    return;
            }
            Collections.sort(list, new Comparator() {
                public int compare(Object o1, Object o2) {
                    return 0 - ((Comparable) ((Map.Entry) (o1)).getValue())
                            .compareTo(((Map.Entry) (o2)).getValue());
                    }
            });
            if(list.size() > 100){
                list = list.subList(0,100);
            }

            for(Map.Entry<String,Double> entry:list){
                String wordB = entry.getKey();
                double weight = entry.getValue();
                int wordcoCount = count.get(wordB);
                result.set("CoCount: "+ wordcoCount + ", Count: "+ wordCount+", Weight:" + weight);
                wordPair.set(key+"&"+wordB);
                context.write(wordPair,result);
            }



        }
    }

    public static void main(String[] args) throws Exception  {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "Word Document Count");
        job.setJarByClass(WordCount.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        //job.setCombinerClass(WordCountReducer.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
