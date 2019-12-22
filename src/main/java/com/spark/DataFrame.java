package com.spark;

import com.alibaba.fastjson.JSON;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author ${张世林}
 * @date 2019/12/22
 * 作用：
 */
public class DataFrame {

    public static void main(String[] args) {
        //sparksession
        SparkSession spark = SparkSession
                .builder()
                .appName("wordCount")
                .master("local[*]")
                .getOrCreate();
        //sparkcontext
        JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());

        JavaRDD<Position> mapJavaRDD = javaSparkContext.textFile("json.json", 5).mapPartitions(list -> {
            List<Position> res = new ArrayList<>();
            while (list.hasNext()) {
                res.add(JSON.parseObject(list.next(), Position.class));
            }
            return res.iterator();
        }).filter(x -> x.getTel().length() == 11);

        //将对象转换为dataset
        Dataset<Row> dataset = spark.createDataFrame(mapJavaRDD, Position.class);
        dataset.createOrReplaceTempView("temp");

        //查询SQL，并取样数据
        Dataset<Row> sampleDataSet = rowDataset(dataset, 500001, 500009).sample(false, 0.5);
        sampleDataSet.show();
//        Row[] collect = sampleDataSet.collect();
//
//        mapJavaRDD.foreach(x -> {
//            String tel = x.getTel();
//            for (Row row : collect) {
//                if (row.getString(3).equals(tel)) {
//
//                }
//            }
//        });




//        Dataset<Row> dataset = spark.read().json("json.json");
//        dataset.show();
//        dataset.createOrReplaceTempView("temp");
//        dataset.sqlContext().sql("select J,W from temp limit 10").show();

    }

    private static Dataset<Row> rowDataset(Dataset<Row> dataset, long startTime, long endTime) {
        String sql = "select * from temp where city_id between " + startTime + " and " + endTime;
        Dataset<Row> rowDataset = dataset.sqlContext().sql(sql);
        return rowDataset;
    }

}
