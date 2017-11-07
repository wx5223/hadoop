package com.shawn.mr.sortindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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
public class SortIndexRemoteSubmit {
    public static class MrMapper extends Mapper<Object, Text, LongWritable, NullWritable> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            Long val = Long.parseLong(line);
            context.write(new LongWritable(val), NullWritable.get());
        }
    }

    public static class MrReducer extends Reducer<LongWritable,NullWritable,LongWritable,LongWritable> {
        private static LongWritable index = new LongWritable(1);

        public void reduce(LongWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            for (NullWritable value : values) {

                context.write(index, key);
                index = new LongWritable(index.get() + 1);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hadoop");
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


        Job job = Job.getInstance(conf, "sort & index txt");
        job.setJarByClass(SortIndexRemoteSubmit.class);
        job.setMapperClass(MrMapper.class);
        //job.setCombinerClass(MrReducer.class);
        job.setReducerClass(MrReducer.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

/**

 2
 32
 654
 32
 15
 756
 65223

 5956
 22
 650
 92

 26
 54
 6


 输出
 1    2
 2    6
 3    15
 4    22
 5    26
 6    32
 7    32
 8    54
 9    92
 10    650
 11    654
 12    756
 13    5956
 14    65223

 */