package com.shawn.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by shawn on 2018/5/21.
 */
public class ProvinceCodeGetIdUDF extends UDF {

    public String evaluate(String key) {
        ProvinceCodeConstant.ProvinceCode provinceCode = ProvinceCodeConstant.provinceCodeMap.get(key);
        if (provinceCode != null) {
            return provinceCode.id;
        }
        return null;
    }

}
