# SparkStreamingDemo
本项目实现了Spark Streaming利用influxdb数据库作为流处理的数据源，并将处理结果存入influxdb的流程
由于influxdb不在Spark内置支持的数据源之列 (that is, beyond Flume, Kafka, Kinesis, files, sockets, etc.)，
因此根据官网教程，需要实现一个自定义receiver, 该类需继承抽象父类，并实现，onStart()，onStop()，store()方法。
实现的自定义receiver类在创建InputDStream时作为参数：
<JavaReceiverInputDStream<String> lines = ssc.receiverStream(new InfluxdbReceiver(sql, 1577434248746L));>
  
 ## influxdbReciver:
 实现的Spark自定义接收器，在流计算创建InputDStream时被调用，实现了间隔自定义时间从influxdb取出新存入的时序数据，并存入Spark缓存。
 
 ## InfluxdbConnection:
 java influxdb操作类，在存取数据时被调用。
 
 ## WordCount:
 数据处理操作主类，本例只是简单实现了取出数据的字符个数统计。
 
 

