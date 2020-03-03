package spark.streaming.demo;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

public class TimeUtil {
    /*
     * 将时间转换为时间戳
     */
    public static long dateToStamp(String s) throws ParseException {
        String res;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = simpleDateFormat.parse(s);
        return date.getTime();
    }

    /*
     * 将时间戳转换为时间
     */
    public static String stampToDate(String s){
        String res;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long lt = new Long(s);
        Date date = new Date(lt);
        res = simpleDateFormat.format(date);
        return res;
    }

    /*
     * 获取List中最大的时间戳
     */
    public long getMaxTimestamp(List<List<Object>> valueList) throws ParseException {
        ArrayList<Long> time = new ArrayList<>();
        if (valueList != null && valueList.size() > 0) {
            for (List<Object> value : valueList) {
                String field1 = value.get(0) == null ? null : value.get(0).toString();
                long timeStamp = dateToStamp(field1);
                time.add(timeStamp);
            }
        }
//        System.out.println(time);
        return Collections.max(time);
    }

    /*
    获取当前时间戳
     */
    public long getCurrentTimestamp(){
        return System.currentTimeMillis();
    }

//    public static void main(String[] args) {
//       long a = new TimeUtil().getCurrentTimestamp();
//        System.out.println(a);
//    }

}
