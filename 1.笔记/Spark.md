# Spark学习笔记

## 一. RDD概述

## 1. RDD基本概念

> ​	**`RDD（Resilient Distributed Dataset）`**叫做 **分布式数据集**，是Spark中最基本的数据抽象；代码中存在的形式是一个抽象类，它代表一个不可变、可分区、里面元素可并行计算的集合。

### 2. RDD的属性

> - 一组分区（Partition），即数据集的基本组成单位；
> - 一个计算每个分区的函数；
> - RDD之间的依赖关系；
> - 一个Partitioner，即RDD的分片函数；
> - 一个列表，存储存取每个Partition的优先位置（Preferred location）；

### 3. RDD的特点

> **`RDD表示只读的分区的数据集；`**
>
> **对RDD进行改动，只能通过RDD的转换操作，由一个RDD得到一个新的RDD，新的RDD包含了从其他RDD衍生所必需的信息；**
>
> **RDDS之间存在依赖关系，RDD的执行时按照血缘关系延时计算的；如果血缘关系较长，可通过持久化RDD来切断血缘关系。**

#### 3.1 分区概念

> 

### 4. Spark三大数据结构

> - RDD：分布式数据集
>
> - 广播变量：分布式只读共享变量
>
> - 累加器：分布式只写共享变量  
>
> ```java
>         //创建一个累加器
>         LongAccumulator longAccumulator = javaSparkContext.sc().longAccumulator();
>         parallelize.foreach(x -> longAccumulator.add(Integer.valueOf(x)));
>         System.out.println(longAccumulator.sum());
> 
> 
>         ArrayList<Map<String, Object>> arrayList = new ArrayList<Map<String, Object>>();
>         for (int i = 0; i < 5; i++) {
>             HashMap<String, Object> map = new HashMap<>();
>             map.put(String.valueOf(i), i * i);
>             arrayList.add(map);
>         }
>         //将ArrayList集合创建成共享变量
>         Broadcast<ArrayList<Map<String, Object>>> broadcast = javaSparkContext.broadcast(arrayList);
> 
>         System.out.println("--------------------------------------------");
>         JavaRDD<String> jsonRDD = javaSparkContext.textFile("json.json", 5);
>         JavaRDD<Map<String, Object>> mapJavaRDD = jsonRDD.mapPartitions(datas -> {
>             List<Map<String, Object>> res = new ArrayList<Map<String, Object>>();
>             while (datas.hasNext()) {
>                 res.add(JSON.parseObject(datas.next(), Map.class));
>             }
>             return res.iterator();
>         });
>         //输出多少条数据
>         long count = mapJavaRDD.count();
>         System.out.println(count);
> 
>         mapJavaRDD.filter(x -> {
>             int city_id = Integer.valueOf(x.get("city_id").toString()) % 500000 % 5;
>             System.out.println(broadcast.getValue().get(city_id));
>             return city_id == 2;
>         }).collect();
> 
> ```
>
> 

- **`coalesce(num)`**  合并当前RDD的分区，主要是当每个分区的数据量较小以后，可以缩小分区数目，合并分区；

- **`repartition(num)`** 从新分区，跟coalesce一致；
- **`union(otherDataset)`**  将两个RDD合并成一个RDD；





## 二. SparkSQL

> ​	Spark SQL是Spark用来处理结构化数据的一个模块，它提供了2个编程抽象：`DataFrame`和`DataSet`，并且作为分布式SQL查询引擎的作用。
>
> - **`DataFrame`** : 数据结构
> - **`DataSet`** : 数据集

### 2.1 DataFrame

> DataFrame只有类型，相当于只有表头，但是没有该列数据的数据类型；
>
> DataFrame相当于是结构化的表格，主要是从其他地方读取文件、读取jdbc等，将数据转换为一张视图，根据该视图来实现SQL语句的查询；
>
> - spark.read.xxx  获取数据源
> - spark.createTempView  将数据源创建为视图
> - spark.sql   在该视图中查询数据，使用SQL的方式
> - 数据源.printSchema   获取表头
> - df.rdd  将DataFrame转换为 RDD对象；

### 2.2 DataSet

> DataSet是一个具有表头，该列的数据类型的一个spark的SQL对象




## spark-submit参数
```$xslt
spark2-submit \      # 第1行
--class com.google.datalake.TestMain \      #第2行
--master yarn \      # 第3行
--deploy-mode client \      # 第4行
--driver-memory 3g \      # 第5行
--executor-memory 2g \      # 第6行
--total-executor-cores 12 \      # 第7行
--jars /home/jars/test-dep-1.0.0.jar,/home/jars/test-dep2-1.0.0.jar,/home/jars/test-dep3-1.0.0.jar \      # 第8行
/home/release/jars/test-sql.jar \      # 第9行
para1 \      # 第10行
para2 \      # 第11行
"test sql" \      # 第12行
parax      # 第13行
```
### 示例分析
- 第1行：指定该脚本是一个spark submit脚本（spark老版本是spark-submit，新版本spark2.x是spark2-submit）；
- 第2行：指定main类的路径；
- 第3行：指定master（使用第三方yarn作为spark集群的master）；
- 第4行：指定deploy-mode（应用模式，driver进程运行在spark集群之外的机器，从集群角度来看该机器就像是一个client）；
- 第5行：分配给driver的内存为3g，也可用m（兆）作为单位；
- 第6行：分配给单个executor进程的内存为2g，也可用m（兆）作为单位；
- 第7行：分配的所有executor核数（executor进程数最大值）；
- 第8行：运行该spark application所需要额外添加的依赖jar，各依赖之间用逗号分隔；
- 第9行：被提交给spark集群执行的application jar；
- 第10～13行：传递给main方法的参数，按照添加顺序依次传入，如果某个参数含有空格则需要使用双引号将该参数扩起来；

### spark submit脚本中各参数顺序的注意事项：
- 每个参数（最后一个参数除外）后需要先空格再使用\表示结尾；
- spark2-submit必须写在最前面；
- class、master yarn、deploy-mode client等使用了--标注的参数的顺序可以相互调整；
- application jar名这个参数必须写在使用了--标注的参数后；
- 向main方法传递的参数必须写在application jar之后。


## 定时任务执行
> 首先，先编写start.sh
```$xslt
#/bin/bash

spark-submit --master yarn \
--name IotIoState5minJob \
--executor-memory 1G \
--executor-cores 1 \
--num-executors 3  \
--driver-memory 1G \
--conf "spark.ui.port=45053" \
--conf "spark.driver.extraJavaOptions=-XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+PrintGCDetails -verbose:gc -XX:+PrintGCTimeStamps" \
--class com.cserver.job.IotIoState5minJob /app/lib/spark-demo-1.0.jar \
``` 
> 其次，  vi /etc/crontab
> 添加，后面是输出打印日志    */5 * * * * root su - hdfs -c /app/start.sh >> /app/spark.log


## Spark内存管理
> Spark内存管理是主要针对Driver与Exceutor两个方面；


