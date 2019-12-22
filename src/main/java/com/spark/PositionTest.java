package com.spark;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author ${张世林}
 * @date 2019/12/22
 * 作用：
 */
public class PositionTest implements Serializable {

    public static void main(String[] args) {
        //创建sparksession
        SparkSession spark = SparkSession.builder().appName("wordCount").master("local[*]").getOrCreate();
        //创建java的sparkContext
        JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());

        //获取到身份证与电话号码的关联关系
        HashMap<String, Object> map = new HashMap<>();
        map.put("500230199611220431", "13903382632,1590287045122");
        map.put("500230199611220432", "15700770826");
        map.put("500230199611220433", "13106048981");
        Broadcast<HashMap<String, Object>> cardTelMap = javaSparkContext.broadcast(map);

        //读取到数据，并将数据转换为position对象
        JavaRDD<Position> mapJavaRDD = javaSparkContext.textFile("json.json", 5).mapPartitions(datas -> {
            List<Position> res = new ArrayList<>();
            while (datas.hasNext()) {
                res.add(JSON.parseObject(datas.next(), Position.class));
            }
            return res.iterator();
        });
        //过滤出身份证数据
        JavaRDD<Position> idCardRDD = mapJavaRDD.filter(x -> x.getJ() != 0 && x.getW() != 0 && x.getTel().length() == 18);
        //过滤出手机号码数据
        JavaRDD<Position> telRDD = mapJavaRDD.filter(x ->  x.getJ() != 0 && x.getW() != 0 && x.getTel().length() == 11);

        //将手机号码RDD创建为DataSet，为了能够使用SQL查询方式
        Dataset<Row> telDataSet = spark.createDataFrame(telRDD, Position.class);
        telDataSet.createOrReplaceGlobalTempView("telView");
//        Broadcast<Dataset<Row>> telDataSetBroad = javaSparkContext.broadcast(telDataSet);

        //该变量主要是要与存储距离相差的所有对象
        Broadcast<List<ReduceData>> lengthListBroad = javaSparkContext.broadcast(new CopyOnWriteArrayList<>());

        //将身份证转成集合，遍历集合，计算该身份证所属手机具有哪一些经纬度差异较大的数据
        idCardRDD.collect().forEach(idCard -> {
            long event_id = idCard.getEvent_id();
            String card = idCard.getTel();
            Object tel = cardTelMap.getValue().get(card);
            String tels = tel != null ? "'" + StringUtils.join(tel.toString().split(","), "','") + "'" : "";
            if (tels != "") {
                rowDataset(telDataSet, rangeTime("-", 10000, event_id), rangeTime("+", 10000, event_id), tels, card, lengthListBroad);
                System.out.println(lengthListBroad.getValue().size());
            }
        });

        //将计算出的距离差距差了多大的集合写入到数据库中lengthListBroad
        Dataset<Row> lengthDataSet = spark.createDataFrame(lengthListBroad.getValue(), ReduceData.class);
        System.out.println(lengthDataSet.count());
        Properties properties = new Properties();
        properties.setProperty("user","root");
        properties.setProperty("password","123456");
        //指定模式，以追加的方式 Overwrite 覆盖   ErrorIfExists存在则报错   Ignore 忽略  append 追加
        lengthDataSet.write().mode(SaveMode.Append).jdbc("jdbc:mysql://127.0.0.1:3306/yien","reduce",properties);


        spark.stop();
        javaSparkContext.stop();
    }

    /**
     * SparkSQL查询对应的数据，并执行经纬度位置差异计算
     * @param dataset
     * @param startTime
     * @param endTime
     * @param tels
     * @return
     */
    private static void rowDataset(Dataset<Row> dataset, long startTime, long endTime, String tels, String card, Broadcast<List<ReduceData>> lengthListBroad) {
        String sql = "select J,W,tel from global_temp.telView where event_id between " + startTime + " and " + endTime + " and tel in (" + tels + ")";
        SQLContext sqlContext = dataset.sqlContext();
        Dataset<Row> rowDataset = sqlContext.sql(sql);
//        Dataset<Row> rowDataset = dataset.where("event_id between " + startTime + " and " + endTime).where("tel in (" + tels + ")").select("j,w,tel");

        rowDataset.foreach(x -> {
            double lng = Double.valueOf(x.get(0).toString());
            double lat = Double.valueOf(x.get(1).toString());
            double lng1 = lng - 0.2;
            double lat1 = lat - 0.2;
            double length = GetDistance(lat, lng, lat1, lng1);
            if (length > 100) {
//                Map<String, Object> map = new HashMap<>();
//                map.put("tel", x.get(1).toString());
//                map.put("J",lng);
//                map.put("W",lat);
//                map.put("J1",lng1);
//                map.put("W1",lat1);
//                map.put("card",card);
                ReduceData builder = ReduceData.builder().lat(lat).lat1(lat1).lng(lng).lng1(lng1).center_val(card).relative_val(x.get(2).toString()).length(length).type("card-tel").build();
                lengthListBroad.getValue().add(builder);
            }
        });
    }

    /**
     * 创建时间范围
     * @param type
     * @param range
     * @param time
     * @return
     */
    private static long rangeTime(String type, long range, long time) {
        if ("+".equals(type)) {
            return time + range;
        }
        return time - range;
    }


    /**
     * 经纬度计算
     */
    private static double EARTH_RADIUS = 6371.393;
    private static double rad(double d)
    {
        return d * Math.PI / 180.0;
    }
    public static double GetDistance(double lat1, double lng1, double lat2, double lng2)
    {
        double radLat1 = rad(lat1);
        double radLat2 = rad(lat2);
        double a = radLat1 - radLat2;
        double b = rad(lng1) - rad(lng2);
        double s = 2 * Math.asin(Math.sqrt(Math.abs(Math.pow(Math.sin(a/2),2) +
                Math.cos(radLat1)*Math.cos(radLat2)*Math.pow(Math.sin(b/2),2))));
        s = s * EARTH_RADIUS;
        s = Math.round(s * 1000);
        return s;
    }

}
