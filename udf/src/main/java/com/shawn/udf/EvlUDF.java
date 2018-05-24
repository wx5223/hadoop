package com.shawn.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by shawn on 2018/5/21.
 */
public class EvlUDF extends UDF {

    public String evaluate(String... values) {
        for (int i = 0; i < values.length; i++) {
            if(!isEmpty(values[i])) {
                return values[i];
            }
        }
        return null;
    }

    private static boolean isEmpty(String str) {
        return str == null || str.length() == 0;
    }

}
