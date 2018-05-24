package com.shawn.mr.commons;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by shawn on 2017/10/31.
 */
public class RemoveDuplicateRemoteSubmit {
    public static class RemoveMapper extends Mapper<Object, Text, Text, NullWritable> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value, NullWritable.get());
        }
    }

    public static class RemoveReducer extends Reducer<Text,NullWritable,Text,NullWritable> {

        public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
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
        conf.set("mapred.jar","target/mr-study-1.0.jar");
        //解决Stack trace: ExitCodeException exitCode=1: /bin/bash: line 0: fg: no job control
        //https://issues.apache.org/jira/browse/MAPREDUCE-5655
        //conf.set("mapreduce.app-submission.cross-platform", "true");


        Job job = Job.getInstance(conf, "remove duplicate txt");
        job.setJarByClass(RemoveDuplicateRemoteSubmit.class);
        job.setMapperClass(RemoveMapper.class);
        job.setCombinerClass(RemoveReducer.class);
        job.setReducerClass(RemoveReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

/**

 2012-3-1 a
 2012-3-2 b
 2012-3-3 c
 2012-3-4 d
 2012-3-5 a
 2012-3-6 b
 2012-3-7 c
 2012-3-3 c

 2012-3-1 b
 2012-3-2 a
 2012-3-3 b
 2012-3-4 d
 2012-3-5 a
 2012-3-6 c
 2012-3-7 d
 2012-3-3 c


 */