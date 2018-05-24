package com.shawn.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by shawn on 2018/5/21.
 */
public class ProvinceCodeGetNameUDF extends UDF {

    public String evaluate(String key) {
        ProvinceCodeConstant.ProvinceCode provinceCode = ProvinceCodeConstant.provinceCodeMap.get(key);
        if (provinceCode != null) {
            return provinceCode.name;
        }
        return null;
    }

}
