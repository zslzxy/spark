package com.spark;

import com.alibaba.fastjson.JSON;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;
import java.util.Map;

/**
 * @author ${张世林}
 * @date 2019/12/20
 * 作用：
 */
public class Count1 {

    public static void main(String[] args) {
        System.out.println(System.currentTimeMillis());
        String tel = "15301312817,13103282516";
        String master = "local[*]";
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster(master);
        sparkConf.setAppName("wordCount");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> jsonRDD = javaSparkContext.textFile("json.json", 5);
        JavaRDD<Map> mapJavaRDD = jsonRDD.map(x -> JSON.parseObject(x, Map.class));
        mapJavaRDD.collect().forEach(System.out::println);

//        JavaRDD<Map> mapJavaRDD = jsonRDD.mapPartitions(new FlatMapFunction<Iterator<String>, Map>() {
//            @Override
//            public Iterator<Map> call(Iterator<String> stringIterator) throws Exception {
//                List<Map> list = new ArrayList<Map>();
//                while (stringIterator.hasNext()) {
//                    String next = stringIterator.next();
//                    list.add(JSON.parseObject(next, Map.class));
//                }
//                return list.iterator();
//            }
//        });

        JavaRDD<Map> javaRDD = mapJavaRDD.filter(x -> {
            System.out.println(x.get("W"));
            return x.get("W") != null;
        });     
//        javaRDD.collect().forEach(System.out::println);
        System.out.println(javaRDD.count());

        JavaPairRDD<Object, Iterable<Map>> groupRDD = mapJavaRDD.groupBy(x -> {
            return x.get("tel");
        });

        JavaPairRDD<Object, Iterable<Map>> filterRDD = groupRDD.filter(x -> {
            return tel.contains(x._1().toString());
        });

        System.out.println(filterRDD.count());

        orther(filterRDD);



        List<Tuple2<Object, Iterable<Map>>> collect1 = filterRDD.collect();
//        collect1.forEach(System.out::println);


        JavaPairRDD<Object, Iterable<Map>> idCardFilterRDD = groupRDD.filter(x -> x._1().toString().length() == 18);
        List<Tuple2<Object, Iterable<Map>>> collect2 = idCardFilterRDD.collect();
//        collect2.forEach(System.out::println);



//        List<Tuple2<Object, Iterable<Map>>> collect1 = groupRDD.collect();


//        JavaRDD<Map> distinctRDD = mapJavaRDD.distinct();
//
//        List<Map> collect = distinctRDD.collect();
//        collect.forEach(System.out::println);
        System.out.println(System.currentTimeMillis());
    }

    private static Object orther(JavaPairRDD<Object, Iterable<Map>> filterRDD) {
        System.out.println("----------" + filterRDD.count());
        List<Tuple2<Object, Iterable<Map>>> take = filterRDD.take(1);
        take.forEach(System.out::println);
        return null;
    }

}
