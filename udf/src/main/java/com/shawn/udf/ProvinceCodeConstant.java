package com.shawn.udf;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by shawn on 2018/5/21.
 */
public class ProvinceCodeConstant {
    public static class ProvinceCode {
        public String code;
        public String id;
        public String name;

        public ProvinceCode(String code, String id, String name) {
            this.code = code;
            this.id = id;
            this.name = name;
        }
    }

    public final static Map<String, ProvinceCode> provinceCodeMap = new HashMap<String, ProvinceCode>();
    static {
        provinceCodeMap.put("410000", new ProvinceCode("410000","1664","河南"));
        provinceCodeMap.put("350000", new ProvinceCode("350000","1262","福建"));
        provinceCodeMap.put("450000", new ProvinceCode("450000","2301","广西"));
        provinceCodeMap.put("220000", new ProvinceCode("220000","629","吉林"));
        provinceCodeMap.put("310000", new ProvinceCode("310000","861","上海"));
        provinceCodeMap.put("320000", new ProvinceCode("320000","881","江苏"));
        provinceCodeMap.put("330000", new ProvinceCode("330000","1010","浙江"));
        provinceCodeMap.put("340000", new ProvinceCode("340000","1123","安徽"));
        provinceCodeMap.put("150000", new ProvinceCode("150000","377","内蒙古"));
        provinceCodeMap.put("460000", new ProvinceCode("460000","2439","海南"));
        provinceCodeMap.put("500000", new ProvinceCode("500000","2469","重庆"));
        provinceCodeMap.put("530000", new ProvinceCode("530000","2832","云南"));
        provinceCodeMap.put("510000", new ProvinceCode("510000","2510","四川"));
        provinceCodeMap.put("520000", new ProvinceCode("520000","2731","贵州"));
        provinceCodeMap.put("420000", new ProvinceCode("420000","1859","湖北"));
        provinceCodeMap.put("230000", new ProvinceCode("230000","707","黑龙江"));
        provinceCodeMap.put("430000", new ProvinceCode("430000","1989","湖南"));
        provinceCodeMap.put("440000", new ProvinceCode("440000","2139","广东"));
        provinceCodeMap.put("360000", new ProvinceCode("360000","1366","江西"));
        provinceCodeMap.put("370000", new ProvinceCode("370000","1489","山东"));
        provinceCodeMap.put("630000", new ProvinceCode("630000","3308","青海"));
        provinceCodeMap.put("540000", new ProvinceCode("540000","2985","西藏"));
        provinceCodeMap.put("140000", new ProvinceCode("140000","235","山西"));
        provinceCodeMap.put("610000", new ProvinceCode("610000","3067","陕西"));
        provinceCodeMap.put("210000", new ProvinceCode("210000","500","辽宁"));
        provinceCodeMap.put("110000", new ProvinceCode("110000","2","北京"));
        provinceCodeMap.put("120000", new ProvinceCode("120000","21","天津"));
        provinceCodeMap.put("130000", new ProvinceCode("130000","40","河北"));
        provinceCodeMap.put("640000", new ProvinceCode("640000","3361","宁夏"));
        provinceCodeMap.put("650000", new ProvinceCode("650000","3394","新疆"));
        provinceCodeMap.put("710000", new ProvinceCode("710000","3510","台湾省"));
        provinceCodeMap.put("810000", new ProvinceCode("810000","3511","香港特别行政区"));
        provinceCodeMap.put("820000", new ProvinceCode("820000","3512","澳门特别行政区"));
        provinceCodeMap.put("620000", new ProvinceCode("620000","3195","甘肃"));
    }
}
