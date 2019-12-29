package com.spark.stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @author ${张世林}
 * @date 2019/12/26
 * 作用：
 */
public class SparkStream1 {

    public static <U> void main(String[] args) {

        /**
         * 第一步：配置SparkConf，
         *     1. 因为 Spark Streaming 应用程序至少有一条线程用于不断的循环结束数据，并且至少有一条线程用于处理
         *              接收的数据（否则的话无线程用于处理数据，随着时间的推移，内存和磁盘都会不堪重负）
         *  2. 对于集群而已，每个 Executor 一般肯定不止一个线程，那对于处理 Spark Streaming应用程序而言，每个 Executor 一般分配多少Core
         *     比较合适？根据我们过去的经验，5个左右的 Core 是最佳的（一个段子分配为基数 Core 表现最佳，）
         */
//        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("sparkStreaming");
        SparkConf conf = new SparkConf().setMaster("local").setAppName("sparkStreaming");

        /**
         * 第二步：创建 SparkStreamingContext，
         *     1.这个 SparkStreaming 应用程序所有功能的起始点和程序调度的核心
         *         SparkStreamingContext 的构建可以基于 SparkConf参数，也可基于持久化的 SaprkStreamingContext的内容来回复过来
         *         （典型的场景是 Driver 奔溃后重新启动，由于 Spark Streaming 具有连续 7*24 小时不间断运行的特征，所有需要在 Driver 重新启动后继续上一次的状态，
         *            此时的状态恢复需要基于曾经的 Checkpoint）
         *     2.在一个Spark Streaming 应用程序中可以创建若干个 SaprkStreamingContext对象，使用下一个 SaprkStreamingContext
         *       之前需要把前面正在运行的 SparkStreamingContext 对象关闭掉，由此，我们获得一个重大的启发 SparkStreaming框架也只是Spark Core上的一个应用程序而言
         *      只不过 Spark Streaming 框架要运行的话需要Spark工程师写业务逻辑处理代码；
         */
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));

        /**
         * 第三步： 创建 Spark Streaming 输入数据来源 input Stream
         *      1.数据输入来源可以基于 File，HDFS， Flume，Kafka，Socket等；
         *      2.在这里我们制定数据来源于网络 Socket端口，Spark Streaming链接上改端口并在运行的时候一直监听该端口的数据（当然该端口服务首先必须存在），并且在后续会根据业务需要不断的
         *        有数据产生（当然对于Spark Streaming 引用程序的运行而言，有无数据其处理流程都是一样的）
         *   3.如果经常在每隔 5 秒钟没有数据的话不断的启动空的 Job 其实是会造成调度资源的浪费，因为彬没有数据需要发生计算；真实的企业级生产环境的代码在具体提交 Job 前会判断是否有数据，如果没有的话
         *   不再提交 Job；
         */

        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("127.0.0.1", 9999);

        /**
         * 第四步：接下来就是 对于 Rdd编程一样基于 DStream进行编程！！！原因是DStream是RDD产生的模板（或者说类）， 在 Saprk Stream发生计算前，其实质是把每个 Batch的DStream的操作翻译
         *         成为 Rdd 的操作！！！
         */

        JavaDStream<String> flatMap = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                String[] split = line.split(" ");
                return (Iterator<String>) Arrays.asList(split);
            }
        });

        JavaPairDStream<String, Integer> mapToPair = flatMap.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });

        JavaPairDStream<String, Integer> reduceByKey = mapToPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });


        /**
         * 此处print并不会直接触发 job 的执行，因为现在的一切都是在 Spark Streaming 框架的控制之下的，对于 Spark Streaming 而言具体是否触发真正的 job 运行
         * 是基设置的  Duration 时间间隔触发
         * 一定要注意的是 Spark Streaming应用程序要想执行具体的Job，对DStream就必须有 output Stream操作
         * output Stream有很多类型的函数触发，类print，saveAsTextFile，saveAsHadoopFile等，最为重要的一个方法是 foreachRDD,因为Spark Streaming处理的结果一般都会放在 Redis，DB，
         * DashBoard等上面，foreachRDD主要就是用来完成这些功能的，而且可以随意的自定义具体数据到底放在那里
         */
        reduceByKey.print();

        /**
         * Spark Streaming 执行于一条新的引擎也就是Driver开始运行，Driver启动的时候是位线程中的，当然其内部有消息接收应用程序本身或者 Executor 中的消息；
         *
         */
        try {
            jsc.start();
            jsc.awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
