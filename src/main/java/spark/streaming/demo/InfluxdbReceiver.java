package spark.streaming.demo;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.influxdb.dto.QueryResult;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;


public class InfluxdbReceiver extends Receiver<String> {

    String sql = null;
    String sql_backup = null;
    long timestamp;

    public InfluxdbReceiver(String sql_, long Timestamp) {
        super(StorageLevel.MEMORY_AND_DISK_2());
        sql = sql_;
        sql_backup = sql_;
        timestamp = Timestamp;
    }

    @Override
    public void onStart() {
        // Start the thread that receives data over a connection
        new Thread(this::receive).start();
    }

    @Override
    public void onStop() {
        // There is nothing much to do as the thread calling receive()
        // is designed to stop by itself if isStopped() returns false
    }

    /*
     * 将时间转换为时间戳
     */
    public long dateToStamp(String s) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        Date date = simpleDateFormat.parse(s);
        return date.getTime();
    }

    /** Create a socket connection and receive data until receiver is stopped */
    private void receive() {

        TimeUtil timeUtil = new TimeUtil();
        try {
            // connect to the db
            InfluxdbConnection connection = new InfluxdbConnection("admin", "admin", "http://127.0.0.1:8086", "data_analysis", "default");
            // Until stopped or connection broken continue reading
            while (!isStopped()) {
                //查询需设置时间限制，避免获取到重复数据
                sql = sql_backup;
                sql = String.format(sql + "where time > %s;", timestamp);
//                System.out.println(sql);
                QueryResult results = connection.query(sql);
                //results.getResults()是同时查询多条SQL语句的返回值，此处我们只有一条SQL，所以只取第一个结果集即可。
                QueryResult.Result oneResult = results.getResults().get(0);
                if (oneResult.getSeries() != null) {
                    //将查询结果转换为list
                    List<List<Object>> valueList = oneResult.getSeries().stream().map(QueryResult.Series::getValues)
                            .collect(Collectors.toList()).get(0);
                    //更新时间戳
                    timestamp = timeUtil.getMaxTimestamp(valueList);
                    //数据存储到Spark缓存中
                    store(valueList.toString());
                }
            }
            connection.close();

            // Restart in an attempt to connect again when server is active again
            restart("Trying to connect again");
        } catch(Throwable t) {
            // restart if there is any other error
            restart("Error receiving data", t);
        }
    }
}