package com.shawn.mr.user;


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.regex.Pattern;

public class UserMapReduce {

    static class RawDataMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Path path = ((FileSplit) context.getInputSplit()).getPath() .getParent();

            String fileName = path.toString();
            System.out.println(fileName);

            if (value == null) {
                return;
            }
            String[] fields = value.toString().split("\u0001", -1);
            if (fields == null || fields.length == 0) {
                return;
            }
            String userId = null;
            if (fields.length == 7) {
                //cloud_user
                /**
                 user_id,nick_name,phone,real_name,email,state,batch_code
                 */
                userId = fields[0];

            } else if (fields.length == 28) {
                //cloud_user_
                /**
                 user_id,sex,birthday,register_time,province,city,county,street,is_first_login,last_login_time,last_login_ip,online_time,weibo_num,photo_num,\
                 photo_total_size,share_num,diary_num,attention_to_num,attention_by_num,id_card,address,riches\
                 ,integral,source,proc_type,proc_nick_name,proc_account,role
                 */
                userId = fields[0];

            } else if (fields.length == 9) {
                //edu_org_user_relation
                /**
                 id,user_id,role,role_type,common_type,platform_id,batch_code,role_nickname,subject_id
                 */
                userId = fields[1];

            } else if (fields.length == 8) {
                //cloud_user_platform
                /**
                 id,user_id,platform_id,create_time,platform_user_id,platform_account,platform_password,batch_code
                 */
                userId = fields[1];
            }

            if (userId == null || !checkUserId(userId)) {
                return;
            }
            context.write(new Text(userId), value);


        }
    }

    static class RawDataReduce extends Reducer<Text,Text,NullWritable,Text>{

        private Map<String, PlatformWritable> platformMap = new HashMap<String, PlatformWritable>();
        private Map<String, String> orgMap = new HashMap<String, String>();

        public void readOrg(Context context) throws IOException{
            String platformPath = context.getConfiguration().get("orgPath");
            FileSystem fs = FileSystem.get(context.getConfiguration());
            // 读取文件列表
            Path filePath =new Path(platformPath);
            FileStatus stats[] = fs.listStatus(filePath);
            String line = "";
            // 依次处理每个文件
            for(int i = 0; i < stats.length; ++i){
                Path inFile =new Path(stats[i].getPath().toString());
                FSDataInputStream fin = fs.open(inFile);
                BufferedReader input = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
                // 处理当前文件数据
                while ((line = input.readLine()) != null) {
                    String[] fields = line.split("\u0001", -1);
                    //build map
                    /**
                     id,code,name,rank
                     */
                    String id = fields[0];
                    String name = fields[2];
                    if (id != null) {
                        orgMap.put(id, name);
                    }

                }
                // 释放
                if (input != null) {
                    input.close();
                    input = null;
                }
                if (fin != null) {
                    fin.close();
                    fin = null;
                }
            }
        /* 测试信息
        System.out.println("userid count" + userIdList.size());
        */
        }

        public void readPlatform(Context context) throws IOException{
            String platformPath = context.getConfiguration().get("platformPath");
            FileSystem fs = FileSystem.get(context.getConfiguration());
            // 读取文件列表
            Path filePath =new Path(platformPath);
            FileStatus stats[] = fs.listStatus(filePath);
            String line = "";
            // 依次处理每个文件
            for(int i = 0; i < stats.length; ++i){
                Path inFile =new Path(stats[i].getPath().toString());
                FSDataInputStream fin = fs.open(inFile);
                BufferedReader input = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
                // 处理当前文件数据
                while ((line = input.readLine()) != null) {
                    String[] fields = line.split("\u0001", -1);
                    //build map
                    /**
                     platform_id,platform_name,is_apply,state,identify,security_key,type,province_id,xxt_url,edu_token,xxt_token
                     ,xxt_password_encoder,xxt_sync_password,roles,sort_num,is_online,check_mode,request_count,is_sync,billing_yn
                     ,network_bill_yn,billing_type,is_cut_over,is_local
                     */
                    PlatformWritable platform = new PlatformWritable(fields[0], fields[1], fields[6], fields[7]);
                    platformMap.put(platform.getPlatformId(), platform);

                }
                // 释放
                if (input != null) {
                    input.close();
                    input = null;
                }
                if (fin != null) {
                    fin.close();
                    fin = null;
                }
            }
        /* 测试信息
        System.out.println("userid count" + userIdList.size());
        */
        }

        @Override
        protected void setup(Context context) throws IOException {
            // 读取初始化数据
            readPlatform(context);
            readOrg(context);
        }
        @Override
        protected void cleanup(Context context) throws IOException {
            // 释放数据
            if(!platformMap.isEmpty()){
                platformMap.clear();
            }
        }

        UserAll ut=new UserAll();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            UserAll u = new UserAll();
            String[] resultArray = new String[52];
            //字段 顺序按 userTotal

            String isTeacher = "0";
            String isStudent = "0";
            String isParent = "0";


            List<String> roleList = new ArrayList<String>();
            List<String> rolePlatformIdList = new ArrayList<String>();
            List<String> roleCommonIdList = new ArrayList<String>();

            String isEdu = "0";
            String isXxt = "0";
            String isHbb = "0";
            String isStl = "0";
            String isZb = "0";
            String isHzj = "0";
            List<String> platformIdList = new ArrayList<String>();
            List<String> platformNameList = new ArrayList<String>();
            List<String> platformTypeList = new ArrayList<String>();

            for (Text value : values) {
                //cloud_user_、cloud_user_、edu_org_user_relation
                if (value == null) {
                    continue;
                }
                String[] fields = value.toString().split("\u0001", -1);
                if (fields == null || fields.length == 0) {
                    continue;
                }
                //cloud_user、cloud_user_、edu_org_user_relation
                if (fields.length == 7) {
                    //cloud_user

                    /**
                     user_id,nick_name,phone,real_name,email,state,batch_code
                     */

                    System.arraycopy(fields, 1, resultArray, 1, 6);
                    /*
                    u.setPhone(fields[1]);
                    u.setEmail(fields[2]);
                    u.setNickName(fields[3]);
                    u.setRealName(fields[4]);
                    u.setState(fields[5]);
                    u.setBatchCode(fields[6]);
                    */

                } else if (fields.length == 28) {
                    //cloud_user_
                    resultArray[0] = fields[0];
                    /**
                     user_id,sex,birthday,register_time,province,city,county,street,is_first_login,last_login_time,last_login_ip,online_time,weibo_num,photo_num,\
                     photo_total_size,share_num,diary_num,attention_to_num,attention_by_num,id_card,address,riches\
                     ,integral,source,proc_type,proc_nick_name,proc_account,role
                     */
                    System.arraycopy(fields, 1, resultArray, 7, 27);
                    /*
                    u.setUserId(fields[0]);
                    u.setSex(fields[1]);
                    u.setBirthday(fields[2]);
                    u.setRegisterTime(fields[3]);
                    u.setProvince(fields[4]);
                    u.setCity(fields[5]);
                    u.setCounty(fields[6]);
                    u.setStreet(fields[7]);
                    u.setIsFirstLogin(fields[8]);
                    u.setLastLoginTime(fields[9]);
                    u.setLastLoginIp(fields[10]);
                    u.setOnlineTime(fields[11]);
                    u.setWeiboNum(fields[12]);
                    u.setPhotoNum(fields[13]);
                    u.setPhotoTotalSize(fields[14]);
                    u.setShareNum(fields[15]);
                    u.setDiaryNum(fields[16]);
                    u.setAttentionToNum(fields[17]);
                    u.setAttentionByNum(fields[18]);
                    u.setIdCard(fields[19]);
                    u.setAddress(fields[20]);
                    u.setRiches(fields[21]);
                    u.setIntegral(fields[22]);
                    u.setSource(fields[23]);
                    u.setProcType(fields[24]);
                    u.setProcNickNam(fields[25]);
                    u.setProcAccount(fields[26]);
                    u.setRole(fields[27]);

                    */

                } else if (fields.length == 9) {
                    //edu_org_user_relation
                    /**
                     id,user_id,role,role_type,common_type,platform_id,batch_code,role_nickname,subject_id
                     */

                    /**
                     通用ID:
                     通用类型1、角色类型8、9、10、11，school_id
                     通用类型1、角色类型6、7，grade_id
                     通用类型1、角色类型5，class_id
                     通用类型2、角色类型1、2、3，class_id
                     */
                    String role = fields[2];
                    /**角色类型:
                    通用类型为1：关联M_ROLE身份基础信息表二级职务id（5：班主任；6：年级组长；7：年级副组长；8：校长；9：副校长；10：教导主任；11：副教导主任）
                    通用类型为2，1：教师；2：学生；3：家长*/
                    String roleType = fields[3];
                    //通用类型：1.身份；2.角色
                    String commonType = fields[4];
                    String platformId = fields[5];

                    roleList.add(roleType);
                    rolePlatformIdList.add(platformId);
                    roleCommonIdList.add(role);

                    if ("2".equals(commonType)) {
                        if ("1".equals(roleType)) {
                            isTeacher = "1";
                        } else if ("2".equals(roleType)) {
                            isStudent = "1";
                        } else if ("3".equals(roleType)) {
                            isParent = "1";
                        }
                    }


                    //u = new UserAll(fields[1], "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", fields[3], fields[4], fields[5], "", "", "", "", "", "", "");

                } else if (fields.length == 8) {
                    //cloud_user_platform
                    /**
                     id,user_id,platform_id,create_time,platform_user_id,platform_account,platform_password,batch_code
                     */
                    //1和教育，2校讯通，3和宝贝，4三通两平台 5直播 6和职教
                    String platformId = fields[2];
                    if (platformId == null || platformId.isEmpty() || platformMap.get(platformId) == null) {
                        return;
                    }
                    PlatformWritable platform = platformMap.get(platformId);
                    String type = platform.getType();
                    if ("1".equals(type)) {
                        isEdu = "1";
                    } else if ("2".equals(type)) {
                        isXxt = "1";
                    } else if ("3".equals(type)) {
                        isHbb = "1";
                    } else if ("4".equals(type)) {
                        isStl = "1";
                    } else if ("5".equals(type)) {
                        isZb = "1";
                    } else if ("6".equals(type)) {
                        isHzj = "1";
                    }
                    platformIdList.add(platformId);
                    platformNameList.add(platform.getPlatformName());
                    platformTypeList.add(type);

                }

            }

            if (resultArray[0] == null || !checkUserId(resultArray[0])) {
                return;
            }
            String roleListStr = StringUtils.join(roleList, "\u0002");
            String rolePlatformIdListStr = StringUtils.join(rolePlatformIdList, "\u0002");
            String roleCommonIdListStr = StringUtils.join(roleCommonIdList, "\u0002");
            resultArray[34] = isTeacher;
            resultArray[35] = isStudent;
            resultArray[36] = isParent;
            resultArray[37] = roleListStr;
            resultArray[38] = rolePlatformIdListStr;
            resultArray[39] = roleCommonIdListStr;

            String platformIdListStr = StringUtils.join(platformIdList, "\u0002");
            String platformNameListStr = StringUtils.join(platformNameList, "\u0002");
            String platformTypeListStr = StringUtils.join(platformTypeList, "\u0002");
            resultArray[40] = isEdu;
            resultArray[41] = isXxt;
            resultArray[42] = isHbb;
            resultArray[43] = isStl;
            resultArray[44] = isZb;
            resultArray[45] = isHzj;
            resultArray[46] = platformIdListStr;
            resultArray[47] = platformNameListStr;
            resultArray[48] = platformTypeListStr;

            // province,city,county,street  11-14
            String provinceId = resultArray[10];
            String cityId = resultArray[11];
            String countyId = resultArray[12];
            String streetId = resultArray[13];
            String provinceName = orgMap.get(provinceId);
            String cityName = orgMap.get(cityId);
            String countyName = orgMap.get(countyId);
            String streetName = orgMap.get(streetId);
            resultArray[49] = provinceName;
            resultArray[50] = cityName;
            resultArray[51] = countyName;
            resultArray[51] = streetName;

            // handle total result array
            String resultStr = StringUtils.join(resultArray, "\u0001");

            context.write(NullWritable.get(),new Text(resultStr));
        }
    }

    public static boolean checkUserId(String str) {
        Pattern pattern = Pattern.compile("^[\\d]*$");
        return pattern.matcher(str).matches();
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 4) {
            System.err.println("params error");
            return;
        }
        Configuration conf = new Configuration();
        /*String path = UserMapReduce.class.getClassLoader().getResource("csv").getPath().toString();
        String platformPath = UserMapReduce.class.getClassLoader().getResource("edu_platform.csv").getPath().toString();
        String orgPath = UserMapReduce.class.getClassLoader().getResource("edu_org.csv").getPath().toString();*/
        conf.set("platformPath", args[0]);
        conf.set("orgPath", args[1]);
        Job job = Job.getInstance(conf);
        job.setJarByClass(UserMapReduce.class);

        job.setMapperClass(RawDataMapper.class);
        job.setReducerClass(RawDataReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        String inputPath = args[2];
        Path[] paths = null;
        if (inputPath != null) {
            String[] inputPathArray = inputPath.split(",");
            paths = new Path[inputPathArray.length];
            for (int i = 0; i < inputPathArray.length; i++) {
                paths[i] = new Path(inputPathArray[i]);
            }
        }
        FileInputFormat.setInputPaths(job,paths);
        FileOutputFormat.setOutputPath(job,new Path(args[3]));
        //,new Path("D:/cloudtest/infile/cloud_user_test.csv"),new Path("D:/cloudtest/infile/edu_org_user_realtion.csv")
        boolean rs = job.waitForCompletion(true);
        System.out.println(rs?0:1);
    }

}
