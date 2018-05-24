package com.shawn.utils;


import org.apache.htrace.fasterxml.jackson.core.JsonFactory;
import org.apache.htrace.fasterxml.jackson.core.JsonProcessingException;
import org.apache.htrace.fasterxml.jackson.core.type.TypeReference;
import org.apache.htrace.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
import org.apache.htrace.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;

/**
 * Json转换工具类
 * Created by Shawn on 2014/5/13.
 */
public class JsonUtils {
    private JsonUtils() {
    }

    public static final ObjectMapper mapper = new ObjectMapper();
    public static final JsonFactory factory = mapper.getFactory();

    static {
        // to enable standard indentation ("pretty-printing"):
        //mapper.enable(SerializationFeature.INDENT_OUTPUT);
        // to allow serialization of "empty" POJOs (no properties to serialize)
        // (without this setting, an exception is thrown in those cases)
        mapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        // to write java.util.Date, Calendar as number (timestamp):
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        // DeserializationFeature for changing how JSON is read as POJOs:

        // to prevent exception when encountering unknown property:
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        // to allow coercion of JSON empty String ("") to null Object value:
        mapper.enable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT);
    }

    /**
     * 对象转json     *
     * @param o
     * @return
     */
    public static String Bean2Json(Object o) {
        String json = null;
        try {
            json = mapper.writeValueAsString(o);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return json;
    }

    /**
     * json转对象
     * @param json
     * @param tClass
     * @param <T>
     * @return
     */
    public static <T> T Json2Bean(String json, Class<T> tClass) {
        T t = null;
        try {
            t = mapper.readValue(json, tClass);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return t;
    }

    /**
     * json转对象（复杂类型）
     * @param json
     * @param typeReference
     * @param <T>
     * @return
     */
    public static <T> T Json2Bean(String json, TypeReference<T> typeReference) {
        T t = null;
        try {
            t = mapper.readValue(json, typeReference);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return t;
    }

    /**
     * 对象通过json转化为另一个对象
     * @param o
     * @param tClass
     * @param <T>
     * @return
     */
    public static <T> T Bean2Bean(Object o, Class<T> tClass) {
        return Json2Bean(Bean2Json(o), tClass);
    }

    /**
     * 对象通过json转化为另一个复杂对象
     * @param o
     * @param typeReference
     * @param <T>
     * @return
     */
    public static <T> T Bean2Bean(Object o, TypeReference<T> typeReference) {
        return Json2Bean(Bean2Json(o), typeReference);
    }
}

