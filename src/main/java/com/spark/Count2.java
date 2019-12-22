package com.spark;

import com.alibaba.fastjson.JSON;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author ${张世林}
 * @date 2019/12/20
 * 作用：
 */
public class Count2 {

    public static void main(String[] args) {
        String master = "local[*]";
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster(master);
        sparkConf.setAppName("wordCount");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        List<String> list = new ArrayList<String>();
        list.add("1");
        list.add("3");
        list.add("2");
        JavaRDD<String> parallelize = javaSparkContext.parallelize(list);
        AtomicInteger sum = new AtomicInteger();

        //创建一个累加器
        LongAccumulator longAccumulator = javaSparkContext.sc().longAccumulator();
        parallelize.foreach(x -> longAccumulator.add(Integer.valueOf(x)));
        System.out.println(longAccumulator.sum());


        ArrayList<Map<String, Object>> arrayList = new ArrayList<Map<String, Object>>();
        for (int i = 0; i < 5; i++) {
            HashMap<String, Object> map = new HashMap<>();
            map.put(String.valueOf(i), i * i);
            arrayList.add(map);
        }
        //将ArrayList集合创建成共享变量
        Broadcast<ArrayList<Map<String, Object>>> broadcast = javaSparkContext.broadcast(arrayList);

        System.out.println("--------------------------------------------");
        JavaRDD<String> jsonRDD = javaSparkContext.textFile("json.json", 5);
        JavaRDD<Map<String, Object>> mapJavaRDD = jsonRDD.mapPartitions(datas -> {
            List<Map<String, Object>> res = new ArrayList<Map<String, Object>>();
            while (datas.hasNext()) {
                res.add(JSON.parseObject(datas.next(), Map.class));
            }
            return res.iterator();
        });
        //输出多少条数据
        long count = mapJavaRDD.count();
        System.out.println(count);

        mapJavaRDD.filter(x -> {
            int city_id = Integer.valueOf(x.get("city_id").toString()) % 500000 % 5;
            System.out.println(broadcast.getValue().get(city_id));
            return city_id == 2;
        }).collect();

    }
}
