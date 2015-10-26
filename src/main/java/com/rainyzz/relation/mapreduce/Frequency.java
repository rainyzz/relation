package com.rainyzz.relation.mapreduce;

import com.google.common.base.Splitter;
import com.rainyzz.relation.core.Count;
import com.rainyzz.relation.core.WordMap;
import com.rainyzz.relation.util.LineReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by rainystars on 10/26/2015.
 */
public class Frequency {
    public static final double E = 2.71828182846;
    public static final double U = 0.1;
    public static final double M = 0.15;
    public static final int HALF_WINDOW = 2;

    public static class FrequencyMapper
            extends Mapper<LongWritable,Text,Text,DoubleWritable> {

        //mapper Log
        private static final Log LOG = LogFactory.getLog(FrequencyMapper.class);

        private DoubleWritable res = new DoubleWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            String sentence = value.toString();
            Map<String,String> article = LineReader.readRecord(sentence, 0, 0);

            String[] abstracts = article.get("abs").split(" ");
            String[] titles = article.get("title").split(" ");
            String[] keywords = article.get("keyword").split(" ");

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
                    res.set(weight);
                    word.set(curWord+"&"+abstracts[j]);
                    context.write(word,res);
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
    public static class FrequencyReducer
            extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {

        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            for(DoubleWritable value:values){
                sum += value.get();
            }
            result.set(sum);
            context.write(key,result);
        }
    }

    public static void main(String[] args) throws Exception  {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "Word Document Count");
        job.setJarByClass(Frequency.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapperClass(FrequencyMapper.class);
        job.setReducerClass(FrequencyReducer.class);
        job.setCombinerClass(FrequencyReducer.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
