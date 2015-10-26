package com.rainyzz.relation.mapreduce;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import com.rainyzz.relation.util.LineReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * Created by rainystars on 10/26/2015.
 */
public class WordDocumentCount {
    public static class WordDocumentCountMapper
            extends Mapper<LongWritable,Text,Text,IntWritable>{

        //mapper Log

        private IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable  key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            String sentence = value.toString();
            Map<String,String> article = LineReader.readRecord(sentence, 0, 0);
            Map<String,String> map = Maps.newHashMap();

            Set<String> words = new HashSet<String>();

            for(String word:Splitter.on(" ").omitEmptyStrings().split(article.get("abs"))){
                words.add(word);
            }
            for(String word:Splitter.on(" ").omitEmptyStrings().split(article.get("title"))){
                words.add(word);
            }
            for(String word:Splitter.on(" ").omitEmptyStrings().split(article.get("keyword"))) {
                words.add(word);
            }


            for(String singleWord : words){
                word.set(singleWord);
                context.write(word,one);
            }
        }
    }
    public static class WordDocumentCountReducer
            extends Reducer<Text,IntWritable,Text,IntWritable>{

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable value:values){
                sum += value.get();
            }
            result.set(sum);
            context.write(key,result);
        }
    }

    public static void main(String[] args) throws Exception  {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "Word Document Count");
        job.setJarByClass(WordDocumentCount.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(WordDocumentCountMapper.class);
        job.setReducerClass(WordDocumentCountReducer.class);
        job.setCombinerClass(WordDocumentCountReducer.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
