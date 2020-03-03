package spark.streaming.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class WordCount {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {

        String sql = "select * from pxy1 ";

        /**
         setMaster("local[3]")必须大于2，不然控制台看不到打印结果
         初始化Spark Streaming程序，必须创建一个StreamingContext对象，
         它是Spark Streaming所有流操作的入口。StreamingContext对象可以用SparkConf对象创建。
         */
        SparkConf sparkConf = new SparkConf().setMaster("local[3]").setAppName("spark.streaming.demo.JavaCustomReceiver");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(1000));

        // 从自定义InfluxdbReceiver中产生一个输入流
        JavaReceiverInputDStream<String> lines = ssc.receiverStream(
                new InfluxdbReceiver(sql, 1577434248746L));

        // 数据处理部分
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey(Integer::sum);

        //处理完的数据写入influxdb
        wordCounts.foreachRDD(rdd -> {
            rdd.foreachPartition(partitionOfRecords -> {
                InfluxdbConnection connection = new InfluxdbConnection("admin", "admin", "http://127.0.0.1:8086", "data_analysis", "autogen");
                while (partitionOfRecords.hasNext()) {
                    Map<String, String> tags = new HashMap<String, String>();
                    Map<String, Object> filds = new HashMap<String, Object>();
                    String value = partitionOfRecords.next().toString();
//                    System.out.println(value);
                    tags.put(value, value);
                    filds.put(value, value);
                    connection.insert("test02", tags, filds);
                }
                connection.close();
            });
        });

        wordCounts.print();
        ssc.start();
        ssc.awaitTermination();
    }
}
