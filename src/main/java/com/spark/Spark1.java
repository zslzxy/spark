package com.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

/**
 * @author ${张世林}
 * @date 2019/12/20
 * 作用：
 */
public class Spark1 {

    public static void main(String[] args) {
        String master="spark://192.168.21.182:7077";
        SparkConf sparkConf = new SparkConf();
//        sparkConf.setMaster(master);
//        sparkConf.setAppName("wordCount");

        JavaSparkContext javaSparkContext = new JavaSparkContext(master,"wordCount",sparkConf);
        List<Integer> list = new ArrayList<>();
        list.add(1);
        list.add(2);
        list.add(3);
        list.add(4);
        JavaRDD<Integer> listRDD = javaSparkContext.parallelize(list, 2);
        listRDD.collect().forEach(System.out::println);
        javaSparkContext.close();
    }

}
