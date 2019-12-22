package com.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.JdbcRDD;
import org.apache.spark.sql.SQLContext;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author ${张世林}
 * @date 2019/12/21
 * 作用：
 */
public class SparkMysql {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        String master = "local[*]";
        sparkConf.setMaster(master);
        sparkConf.setAppName("wordCount");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://localhost:3306/yien";
        String userName = "root";
        String password = "123456";

        String sql = "select user_card_no,user_mobile_no from yien_user";
//        String sql = "INSERT INTO `yien`.`my_rdd`(`id`, `J`, `W`, `tel`, `event_id`, `city_id`) VALUES (?, ?, ?, ?, ?, ?)";
        JdbcRDD<Map> mapJdbcRDD = new JdbcRDD<>(javaSparkContext.sc(), () -> {
            Connection connection = null;
            try {
                Class.forName(driver).newInstance();
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            try {
                connection = DriverManager.getConnection(url, userName, password);
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return connection;
        }, sql, 1, 3, 2, res -> {
            HashMap<Object, Object> map = new HashMap<>();
            try {
                String str1 = res.getString(1);
                map.put("idCard", str1);
                String str2 = res.getString(2);
                map.put("tel", str2);
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return map;
        }, null);

//        Map[] collect = mapJdbcRDD.collect();
//        for (Map map : collect) {
//            for (Object o : map.keySet()) {
//                System.out.print(o + ":" + map.get(o));
//            }
//            System.out.println();
//        }


        javaSparkContext.stop();
    }



}
