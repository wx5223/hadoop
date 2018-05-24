package com.shawn.mr.user;


public class UserAll {
    //用户总表
    private String userId;
    private String nickName;
    private String phone;
    private String realName;
    private String email;
    private String state;
    private String batchCode;

    //用户分表
    private String sex;
    private String birthday;
    private String registerTime;
    private String province;
    private String city;
    private String county;
    private String street;
    private String isFirstLogin;
    private String lastLoginTime;
    private String lastLoginIp;
    private String onlineTime;
    private String weiboNum;
    private String photoNum;
    private String photoTotalSize;
    private String shareNum;
    private String diaryNum;
    private String attentionToNum;
    private String attentionByNum;
    private String idCard;
    private String address;
    private String riches;
    private String integral;
    private String source;//用户来源
    private String procType;
    private String procNickNam;
    private String procAccount;
    private String role;


    //角色相关
    private String isTeacher;
    private String isStudent;
    private String isParent;
    private String roleListStr;//角色类型：1.教师，2.学生，3.家长
    private String rolePlatformIdListStr;//通用类型：1.身份；2.角色
    private String roleCommonIdListStr;

    //平台相关;
    private String isEdu;
    private String isXxt;
    private String isHbb;
    private String isStl;
    private String isZb;
    private String isHzj;
    private String platformIdListStr;
    private String platformNameListStr;
    private String platformTypeListStr;
    private String platformUserIdListStr;

    //省市相关
    private String provinceName;
    private String cityName;
    private String countyName;
    private String streetName;
    private String provinceCode;



}
