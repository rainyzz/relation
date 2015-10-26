package com.rainyzz.relation.mapreduce;

import com.google.common.base.Splitter;
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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by rainystars on 10/26/2015.
 */
public class WordCoCount {
    public static class WordCoCountMapper
            extends Mapper<LongWritable,Text,Text,IntWritable> {

        //mapper Log
        private static final Log LOG = LogFactory.getLog(WordCoCountMapper.class);

        private IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            String sentence = value.toString();
            Map<String,String> article = LineReader.readRecord(sentence, 0, 0);
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

            for(String wordA : words) {
                for (String wordB : words) {
                    if (wordA.equals(wordB)) {
                        continue;
                    }
                    word.set(wordA + "&" + wordB);
                    context.write(word, one);
                }

            }
        }
    }
    public static class WordCoCountReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {

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
        job.setJarByClass(WordCoCount.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(WordCoCountMapper.class);
        job.setReducerClass(WordCoCountReducer.class);
        job.setCombinerClass(WordCoCountReducer.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
