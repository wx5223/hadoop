package com.shawn.mr.childparent;

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
import java.util.ArrayList;
import java.util.List;

/**
 * Created by shawn on 2017/10/31.
 */
public class ChildParentRemoteSubmit {
    public static class ChildParent implements Writable {
        private Integer relationship;
        private String name;

        public Integer getRelationship() {
            return relationship;
        }

        public void setRelationship(Integer relationship) {
            this.relationship = relationship;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public ChildParent() {
        }

        public ChildParent(Integer relationship, String name) {
            this.relationship = relationship;
            this.name = name;
        }

        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeInt(relationship);
            dataOutput.writeUTF(name);
        }

        public void readFields(DataInput dataInput) throws IOException {
            relationship = dataInput.readInt();
            name = dataInput.readUTF();

        }
    }
    public static class MrMapper extends Mapper<Object, Text, Text, ChildParent> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] arrays = line.split(" ");
            if (arrays != null && arrays.length == 2) {
                //合法数据
                String child = arrays[0];
                String parent = arrays[1];
                if (child.equalsIgnoreCase("child")) {
                    return;
                }
                context.write(new Text(parent), new ChildParent(1, child));
                context.write(new Text(child), new ChildParent(2, parent));
            }

        }
    }

    public static class MrReducer extends Reducer<Text, ChildParent,Text,Text> {
        private static LongWritable index = new LongWritable(1);

        public void reduce(Text key, Iterable<ChildParent> values, Context context) throws IOException, InterruptedException {
            List<String> grandChild = new ArrayList<String>();
            List<String> grandParent = new ArrayList<String>();
            for (ChildParent value : values) {
                if (value.getRelationship() == 1) {
                    grandChild.add(value.getName());
                } else {
                    grandParent.add(value.getName());
                }
            }

            for (String gc : grandChild) {
                for (String gp : grandParent) {
                    context.write(new Text(gc), new Text(gp));
                }
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
        job.setJarByClass(ChildParentRemoteSubmit.class);
        job.setMapperClass(MrMapper.class);
        //job.setCombinerClass(MrReducer.class);
        job.setReducerClass(MrReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ChildParent.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

/**

 child        parent
 Tom        Lucy
 Tom        Jack
 Jone        Lucy
 Jone        Jack
 Lucy        Mary
 Lucy        Ben
 Jack        Alice
 Jack        Jesse
 Terry        Alice
 Terry        Jesse
 Philip        Terry
 Philip        Alma
 Mark        Terry
 Mark        Alma


 grandchild        grandparent
 Tom            　　Alice
 Tom            　　Jesse
 Jone            　　Alice
 Jone           　　 Jesse
 Tom            　　Mary
 Tom            　　Ben
 Jone           　　 Mary
 Jone           　　 Ben
 Philip          　　  Alice
 Philip            　　Jesse
 Mark           　　 Alice
 Mark           　　 Jesse

 */