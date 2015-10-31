package com.rainyzz.relation.mapreduce;

import com.google.common.base.Splitter;
import com.google.common.collect.Maps;

import com.rainyzz.relation.util.LineReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

public class WordCount {
    public static class WordCountMapper extends Mapper<LongWritable,Text,Text,Text> {
        public static final double E = 2.71828182846;
        public static final double U = 0.1;
        public static final double M = 0.15;
        public static final int HALF_WINDOW = 2;

        private Text result = new Text();
        private Text word = new Text();

        private void otherColumnClac(String[] keywords,String curWord,Context context) throws IOException, InterruptedException{
            int curLen = curWord.length();

            for(String keyword:keywords){
                if(curWord.equals(keyword)){
                    continue;
                }
                int keywordLen = keyword.length();
                if(keywordLen == 0){
                    continue;
                }
                double weight = U * Math.pow(E,- U * (M * curLen / keywordLen));
                result.set("@" + keyword + "@" + weight);
                word.set(curWord);
                context.write(word, result);
            }

        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String sentence = value.toString();
            Map<String, String> article = LineReader.readRecord(sentence, 0, 0);
            Set<String> words = new HashSet<String>();

            for (String word : Splitter.on(" ").omitEmptyStrings().split(article.get("abs"))) {
                words.add(word);
            }
            for (String word : Splitter.on(" ").omitEmptyStrings().split(article.get("title"))) {
                words.add(word);
            }
            for (String word : Splitter.on(" ").omitEmptyStrings().split(article.get("keyword"))) {
                words.add(word);
            }

            String[] abstracts = article.get("abs").split(" ");
            String[] titles = article.get("title").split(" ");
            String[] keywords = article.get("keyword").split(" ");

            // 统计单个词出现的文档个数
            for (String w : words) {
                word.set(w);
                result.set("1");
                context.write(word, result);
            }

            // 统计词对出现的文档个数
            for (String wordA : words) {
                for (String wordB : words) {
                    if (wordA.equals(wordB)) {
                        continue;
                    }
                    word.set(wordA);
                    result.set("&" + wordB + "@" + 1);
                    context.write(word, result);
                }
            }

            // LAR Model
            for (int i = 0; i < abstracts.length; i++) {
                String curWord = abstracts[i];

                int windowStart = i - HALF_WINDOW < 0 ? 0 : i - HALF_WINDOW;
                int windowEnd = i + HALF_WINDOW > abstracts.length ? abstracts.length : i + HALF_WINDOW;
                int windowMid = i;

                //对词窗内的词进行统计 windowWeight
                for (int j = windowStart; j < windowEnd; j++) {
                    if (curWord.equals(abstracts[j])) {
                        continue;
                    }
                    int distance = Math.abs(i - j);
                    double weight = U * Math.pow(E, -U * distance);
                    result.set("@" + abstracts[j] + "@" + weight);
                    word.set(curWord);
                    context.write(word, result);
                }
                //统计单个词语出现的次数
                result.set("#1");
                word.set(curWord);
                context.write(word,result);

                //对于当前词，其与所有其他字段中词语都有关系。 diffCoCount
                otherColumnClac(keywords, curWord, context);
                otherColumnClac(titles,curWord,context);

            }
        }
    }


    /*public static class WordCountCombiner extends Reducer<Text, Text, Text, Text> {
            private Text result = new Text();

            public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
                    InterruptedException {
                int wordCount = 0;
                Map<String, Integer> count = Maps.newHashMap();
                Map<String, Double> frequency = Maps.newHashMap();
                for (Text value : values) {
                    String v = value.toString();
                    if (v.startsWith("&")) {
                        //CoCount
                        String wordB = v.substring(1, v.length()).split("@")[0];
                        String num = v.substring(1, v.length()).split("@")[1];
                        if ("".equals(wordB) || "".equals(num)) {
                            continue;
                        }
                        int n = 0;
                        try {
                            n = Integer.valueOf(num);
                        } catch (Exception e) {
                            continue;
                        }

                        if (count.containsKey(wordB)) {
                            count.put(wordB, count.get(wordB) + n);
                        } else {
                            count.put(wordB, n);
                        }
                    } else if (v.startsWith("@")) {
                        //Frequency
                        String wordB = v.substring(1, v.length()).split("@")[0];
                        String num = v.substring(1, v.length()).split("@")[1];

                        double n = 0;
                        try {
                            n = Double.valueOf(num);
                        } catch (Exception e) {
                            continue;
                        }
                        if (frequency.containsKey(wordB)) {
                            frequency.put(wordB, frequency.get(wordB) + n);
                        } else {
                            frequency.put(wordB, n);
                        }

                    } else {
                        //Count
                        try {
                            int intV = Integer.valueOf(v);
                            wordCount += intV;
                        } catch (Exception e) {
                            continue;
                        }
                    }
                    //WordCount
                    result.set(String.valueOf(wordCount));
                    context.write(key, result);

                    //WordCoCount
                    for (String wordB : count.keySet()) {

                        int weight = count.get(wordB);
                        result.set("&" + wordB + "@" + String.valueOf(weight));
                        context.write(key, result);
                    }

                    //Frequency
                    for (String wordB : frequency.keySet()) {

                        double weight = frequency.get(wordB);
                        result.set("@" + wordB + "@" + String.valueOf(weight));
                        context.write(key, result);
                    }
                }
            }
        }*/


        public static class WordCountReducer extends Reducer<Text, Text, Text, Text> {

            private Text result = new Text();
            private Text wordPair = new Text();

            public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
                    InterruptedException {
                int wordDocCount = 0;
                int wordCount = 0;
                Map<String, Integer> pairDocCounts = Maps.newHashMap();
                Map<String, Double> windowWeights = Maps.newHashMap();
                for (Text value : values) {
                    String v = value.toString();
                    if (v.startsWith("&")) {
                        //Pair Document Count
                        String wordB = v.substring(1, v.length()).split("@")[0];
                        String num = v.substring(1, v.length()).split("@")[1];
                        if ("".equals(wordB) || "".equals(num)) {
                            continue;
                        }
                        int n = 0;
                        try {
                            n = Integer.valueOf(num);
                        } catch (Exception e) {
                            continue;
                        }

                        if (pairDocCounts.containsKey(wordB)) {
                            pairDocCounts.put(wordB, pairDocCounts.get(wordB) + n);
                        } else {
                            pairDocCounts.put(wordB, n);
                        }
                    } else if (v.startsWith("@")) {
                        //Window Weight
                        String wordB = v.substring(1, v.length()).split("@")[0];
                        String num = v.substring(1, v.length()).split("@")[1];

                        double n = 0;
                        try {
                            n = Double.valueOf(num);
                        } catch (Exception e) {
                            continue;
                        }
                        if (windowWeights.containsKey(wordB)) {
                            windowWeights.put(wordB, windowWeights.get(wordB) + n);
                        } else {
                            windowWeights.put(wordB, n);
                        }

                    }else if (v.startsWith("#")){
                        //Word Count
                        try {
                            int intV = Integer.valueOf(v.substring(1));
                            wordCount += intV;
                        } catch (Exception e) {
                            continue;
                        }
                    }else{
                        //Word Document Count
                        try {
                            int intV = Integer.valueOf(v);
                            wordDocCount += intV;
                        } catch (Exception e) {
                            continue;
                        }
                    }
                }
                // Final Weight
                Map<String, Double> rst = Maps.newHashMap();
                for (String wordB : windowWeights.keySet()) {

                    double windowWeight = windowWeights.get(wordB);
                    int pairDocCount = 0;
                    if (pairDocCounts.containsKey(wordB)) {
                        pairDocCount = pairDocCounts.get(wordB);
                    } else {
                        continue;
                    }
                    if(wordCount == 0 || wordDocCount == 0){
                        continue;
                    }

                    double weight = windowWeight / wordCount
                            * Math.log10(pairDocCount * 1.0 / wordDocCount + 1);

                    rst.put(wordB, weight);
                }

                // Result Sort
                List<Map.Entry> list = new ArrayList<Map.Entry>(rst.entrySet());
                if (list.size() <= 4) {
                    return;
                }
                Collections.sort(list, new Comparator() {
                    public int compare(Object o1, Object o2) {
                        return 0 - ((Comparable) ((Map.Entry) (o1)).getValue())
                                .compareTo(((Map.Entry) (o2)).getValue());
                    }
                });
                if (list.size() > 100) {
                    list = list.subList(0, 100);
                }

                // Output
                for (Map.Entry<String, Double> entry : list) {
                    String wordB = entry.getKey();
                    double weight = entry.getValue();
                    int pairDocCount = pairDocCounts.get(wordB);
                    result.set("pairDocCount: " + pairDocCount + ", wordDocCount: " + wordDocCount+
                            ", wordCount: "+wordCount +", windowWeight: " + windowWeights.get(wordB)+", Result:" + weight);
                    wordPair.set(key + "&" + wordB);
                    context.write(wordPair, result);
                }


            }
        }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "Word Document Count");
        job.setJarByClass(WordCount.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        //job.setCombinerClass(WordCountCombiner.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
