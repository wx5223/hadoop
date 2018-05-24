package com.shawn.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by shawn on 2017/10/31.
 */
public class WordCountv1RemoteSubmit {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        //设置读写hdfs，不设置可以完全本地运行
        //权限设置 -DHADOOP_USER_NAME=hadoop
        //core-site.xml
        conf.set("fs.defaultFS", "hdfs://hadoop1:9000");

        //设置yarn远程运行（不设置可以只读hdfs 其他本地）
        conf.set("mapreduce.framework.name", "yarn");
        //设置RN的主机 yarn-site.xml
        conf.set("yarn.resourcemanager.hostname", "hadoop1");
        //提交yarn执行需要指定jar包，不然会提示找不到类
        conf.set("mapred.jar","target/hadoop-study-1.0.jar");
        //解决Stack trace: ExitCodeException exitCode=1: /bin/bash: line 0: fg: no job control
        //https://issues.apache.org/jira/browse/MAPREDUCE-5655
        //conf.set("mapreduce.app-submission.cross-platform", "true");


        Job job = Job.getInstance(conf, "word count v1");
        job.setJarByClass(WordCountv1RemoteSubmit.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
