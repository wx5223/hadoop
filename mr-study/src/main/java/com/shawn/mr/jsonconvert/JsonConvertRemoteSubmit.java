package com.shawn.mr.jsonconvert;

import com.google.common.collect.Lists;
import com.shawn.utils.JsonUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * Created by shawn on 2017/10/31.
 */
public class JsonConvertRemoteSubmit {

    public static class MrMapper extends Mapper<Object, Text, Text, NullWritable> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            Map jsonMap = JsonUtils.Json2Bean(line, Map.class);
            if (jsonMap.size() == 8) {
                ArrayList<String> stringArrayList = new ArrayList<String>();
                for (Object val : jsonMap.values()) {
                    if (val instanceof String) {
                        stringArrayList.add((String) val);
                    } else {
                        //异常数据
                        continue;
                    }
                }
                String result = StringUtils.join(stringArrayList, ",");
                context.write(new Text(result), NullWritable.get());
            }


        }
    }

    public static class MrReducer extends Reducer<Text, NullWritable,Text,NullWritable> {
        private static LongWritable index = new LongWritable(1);

        public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
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
        conf.set("mapred.jar","mr-study/target/mr-study-1.0.jar");
        //解决Stack trace: ExitCodeException exitCode=1: /bin/bash: line 0: fg: no job control
        //https://issues.apache.org/jira/browse/MAPREDUCE-5655
        //conf.set("mapreduce.app-submission.cross-platform", "true");


        Job job = Job.getInstance(conf, "json convert job");
        job.setJarByClass(JsonConvertRemoteSubmit.class);
        job.setMapperClass(MrMapper.class);
        //job.setCombinerClass(MrReducer.class);
        job.setReducerClass(MrReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

/**

 {"operTime":"2017-11-07 09:07:28","logId":"a1efba5d-ec1f-44f5-b782-43e087ac51bd","orderId":"00001510016848364741","visit
 Id":"3289DC1828FA76C42AD5B721D1BE197D.xxt-pay","orderTime":"2017-11-07 09:07:28","orderMoney":"100","provinceId":"2139",
 "provinceName":"广东"}
 {"operTime":"2017-11-07 09:07:28","logId":"ac6852d8-bf0b-43a1-aa7f-4d0f130bf8b4","orderId":"00001510016848678543","visit
 Id":"3289DC1828FA76C42AD5B721D1BE197D.xxt-pay","orderTime":"2017-11-07 09:07:28","orderMoney":"100","provinceId":"1989",
 "provinceName":"湖南"}
 {"operTime":"2017-11-07 09:07:31","logId":"6e9b7ab9-16b6-45cb-9132-fd727d981596","orderId":"00001510016851130752","visit
 Id":"A1E19EC7C7B47282CDE9C32BCBEEEED0.xxt-pay","orderTime":"2017-11-07 09:07:31","orderMoney":"100","provinceId":"2139",
 "provinceName":"广东"}

 */