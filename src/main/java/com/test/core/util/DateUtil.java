package com.test.core.util;


import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

/**
 * @author:Neptune
 * @Description:DateUtil 提供一些常用的时间想法的方法
 */
public final class DateUtil {
/**
 *  几个时间名词：

（1）GMT:格林威治标准时间

（2）UTC:世界协调时间

（3）DST:夏日节约时间

（4）CST:中国标准时间
 */
    //日期时间类型格式
	public static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
	public static final String DATETIME_FORMAT_ALL = "yyyy-MM-dd HH:mm:ss.SSS";

    //日期类型格式
    public static final String DATE_FORMAT = "yyyy-MM-dd";

    //时间类型的格式
    public static final String TIME_FORMAT = "HH:mm:ss";
    public static final String TIME_FORMAT_ALL = "HH:mm:ss.SSS";
    // UTC世界协调时间 格式，默认是ISO 8601标准，例如2015-02-27T00:07Z(Z零时区)、2015-02-27T08:07+08:00(+08:00东八区)
    public static final String UTC_Z_FORMAT="yyyy-MM-dd'T'HH:mm:ss'Z'";
    public static final String UTC_Z_FORMAT_ALL="yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    
    public static final String UTC_08_FORMAT="yyyy-MM-dd'T'HH:mm:ss'+08:00'";
    public static final String UTC_08_FORMAT_ALL="yyyy-MM-dd'T'HH:mm:ss.SSS'+08:00'";

    //注意SimpleDateFormat不是线程安全的
    private static ThreadLocal<SimpleDateFormat> ThreadDateTime = new ThreadLocal<SimpleDateFormat>();
    private static ThreadLocal<SimpleDateFormat> ThreadDate = new ThreadLocal<SimpleDateFormat>();
    private static ThreadLocal<SimpleDateFormat> ThreadTime = new ThreadLocal<SimpleDateFormat>();

    private static SimpleDateFormat DateTimeInstance() {
        SimpleDateFormat df = ThreadDateTime.get();
        if (df == null) {
            df = new SimpleDateFormat(DATETIME_FORMAT);
            ThreadDateTime.set(df);
        }
        return df;
    }

    private static SimpleDateFormat DateInstance() {
        SimpleDateFormat df = ThreadDate.get();
        if (df == null) {
            df = new SimpleDateFormat(DATE_FORMAT);
            ThreadDate.set(df);
        }
        return df;
    }

    private static SimpleDateFormat TimeInstance() {
        SimpleDateFormat df = ThreadTime.get();
        if (df == null) {
            df = new SimpleDateFormat(TIME_FORMAT);
            ThreadTime.set(df);
        }
        return df;
    }


    /**
     * 获取当前日期时间
     *
     * @return 返回当前时间的字符串值
     */
    public static String currentDateTime() {
        return DateTimeInstance().format(new Date());
    }

    /**
     * 将指定的时间格式化成出返回
     *
     * @param date
     * @return
     */
    public static String dateTime(Date date) {
        return DateTimeInstance().format(date);
    }

    /**
     * 将指定的字符串解析为时间类型
     *
     * @param datestr
     * @return
     * @throws ParseException
     */
    public static Date dateTime(String datestr) throws ParseException {
        return DateTimeInstance().parse(datestr);
    }

    /**
     * 获取当前的日期
     *
     * @return
     */
    public static String currentDate() {
        return DateInstance().format(new Date());
    }

    /**
     * 将指定的时间格式化成出返回
     *
     * @param date
     * @return
     */
    public  static String date(Date date) {
        return DateInstance().format(date);
    }
    
    
    
    public  static String dateStr_UTC_Z(Date date) {
    	SimpleDateFormat df = new SimpleDateFormat(UTC_Z_FORMAT);
    	df.setTimeZone(TimeZone.getTimeZone("UTC"));
    	return df.format(date);
    }
    
    public  static Date date_UTC_Z(String dateStr) {
    	SimpleDateFormat df = new SimpleDateFormat(UTC_Z_FORMAT);
    	df.setTimeZone(TimeZone.getTimeZone("UTC"));
    	try {
			return df.parse(dateStr);
		} catch (ParseException e) {
			e.printStackTrace();
		}
    	return null;
    }
    
    
    public  static String dateStr_UTC_Z_ALL(Date date) {
    	SimpleDateFormat df = new SimpleDateFormat(UTC_Z_FORMAT_ALL);
    	df.setTimeZone(TimeZone.getTimeZone("UTC"));
    	return df.format(date);
    }
    
    public  static Date date_UTC_Z_ALL(String dateStr) {
    	SimpleDateFormat df = new SimpleDateFormat(UTC_Z_FORMAT_ALL);
    	df.setTimeZone(TimeZone.getTimeZone("UTC"));
    	try {
			return df.parse(dateStr);
		} catch (ParseException e) {
			e.printStackTrace();
		}
    	return null;
    }
    
    
    public  static String dateStr_UTC_08(Date date) {
    	SimpleDateFormat df = new SimpleDateFormat(UTC_08_FORMAT_ALL);
    	return df.format(date);
    }
    
    public  static String dateStr_CST2UTC(String cstTime){
    
		try {
			SimpleDateFormat sdf = new SimpleDateFormat(DATETIME_FORMAT);
			return dateStr_UTC_Z(sdf.parse(cstTime));
		} catch (ParseException e) {
			e.printStackTrace();
		}
    return null;
    }
    
    public  static String dateStr_UTC2CST(String utcTime){
    	String utcTimePattern =DATE_FORMAT;
        String subTime = utcTime.substring(10);//UTC时间格式以 yyyy-MM-dd 开头,将utc时间的前10位截取掉,之后是含有多时区时间格式信息的数据

        //处理当后缀为:+8:00时,转换为:+08:00 或 -8:00转换为-08:00
        if(subTime.indexOf("+") != -1){
            subTime = changeUtcSuffix(subTime, "+");
        }
        if(subTime.indexOf("-") != -1){
            subTime = changeUtcSuffix(subTime, "-");
        }
        utcTime = utcTime.substring(0, 10) + subTime;

        //依据传入函数的utc时间,得到对应的utc时间格式
        //步骤一:处理 T
        if(utcTime.indexOf("T") != -1){
            utcTimePattern = utcTimePattern + "'T'";
        }

        //步骤二:处理毫秒SSS
        if(utcTime.indexOf(".") != -1){
            utcTimePattern = utcTimePattern + "HH:mm:ss.SSS";
        }else{
            utcTimePattern = utcTimePattern + "HH:mm:ss";
        }

        //步骤三:处理时区问题
        if(subTime.indexOf("+") != -1 || subTime.indexOf("-") != -1){
            utcTimePattern = utcTimePattern + "XXX";
        }
        else if(subTime.indexOf("Z") != -1){
            utcTimePattern = utcTimePattern + "'Z'";
        }

        if("yyyy-MM-dd HH:mm:ss".equals(utcTimePattern) || "yyyy-MM-dd HH:mm:ss.SSS".equals(utcTimePattern)){
            return utcTime;
        }

        SimpleDateFormat utcFormater = new SimpleDateFormat(utcTimePattern);
        utcFormater.setTimeZone(TimeZone.getTimeZone("UTC"));
        Date gpsUtcDate = null;
        try {
            gpsUtcDate = utcFormater.parse(utcTime);
        } catch (Exception e) {
            return utcTime;
        }
        SimpleDateFormat localFormater = new SimpleDateFormat(DATETIME_FORMAT);
        localFormater.setTimeZone(TimeZone.getDefault());
        String localTime = localFormater.format(gpsUtcDate.getTime());
        return localTime;
    }
    
    
    /**
     * 函数功能描述:获取本地时区的表示(比如:第八区-->+08:00)
     * @return
     */
    public static String getTimeZoneByNumExpress(){
        Calendar cal = Calendar.getInstance();
        TimeZone timeZone = cal.getTimeZone();
        int rawOffset = timeZone.getRawOffset();
        int timeZoneByNumExpress = rawOffset/3600/1000;
        String timeZoneByNumExpressStr = "";
        if(timeZoneByNumExpress > 0 && timeZoneByNumExpress < 10){
            timeZoneByNumExpressStr = "+" + "0" + timeZoneByNumExpress + ":" + "00";
        }
        else if(timeZoneByNumExpress >= 10){
            timeZoneByNumExpressStr = "+" + timeZoneByNumExpress + ":" + "00";
        }
        else if(timeZoneByNumExpress > -10 && timeZoneByNumExpress < 0){
            timeZoneByNumExpress = Math.abs(timeZoneByNumExpress);
            timeZoneByNumExpressStr = "-" + "0" + timeZoneByNumExpress + ":" + "00";
        }else if(timeZoneByNumExpress <= -10){
            timeZoneByNumExpress = Math.abs(timeZoneByNumExpress);
            timeZoneByNumExpressStr = "-" + timeZoneByNumExpress + ":" + "00";
        }else{
            timeZoneByNumExpressStr = "Z";
        }
        return timeZoneByNumExpressStr;
    } 
    
    /**
     * 函数功能描述:修改时间格式后缀
     * 函数使用场景:处理当后缀为:+8:00时,转换为:+08:00 或 -8:00转换为-08:00
     * @param subTime
     * @param sign
     * @return
     */
    private static String changeUtcSuffix(String subTime, String sign){
        String timeSuffix = null;
        String[] splitTimeArrayOne = subTime.split("\\" + sign);
        String[] splitTimeArrayTwo = splitTimeArrayOne[1].split(":");
        if(splitTimeArrayTwo[0].length() < 2){
            timeSuffix = "+" + "0" + splitTimeArrayTwo[0] + ":" + splitTimeArrayTwo[1];
            subTime = splitTimeArrayOne[0] + timeSuffix;
            return subTime;
        }
        return subTime;
    }
    

    /**
     * 将指定的字符串解析为时间类型
     *
     * @param dateStr
     * @return
     * @throws ParseException
     */
    public  static Date date(String dateStr) {
    	try{
    		return DateInstance().parse(dateStr);
    	}catch (Exception e) {
			e.printStackTrace();
		}
    	return null;
    }

    /**
     * 获取当前的时间
     *
     * @return
     */
    public  static String currentTime() {
        return TimeInstance().format(new Date());
    }

    /**
     * 讲指定的时间格式化成出返回
     *
     * @param date
     * @return
     */
    public  static String time(Date date) {
        return TimeInstance().format(date);
    }

    /**
     * 将指定的字符串解析为时间类型
     *
     * @param dateStr
     * @return
     * @throws ParseException
     */
    public  static Date time(String dateStr) throws ParseException {
        return TimeInstance().parse(dateStr);
    }


    /**
     * 在当前时间的基础上加或减去year年
     *
     * @param year
     * @return
     */
    public  static Date year(int year) {
        java.util.Calendar Cal = java.util.Calendar.getInstance();
        Cal.setTime(new Date());
        Cal.add(Calendar.YEAR, year);
        return Cal.getTime();
    }

    /**
     * 在指定的时间上加或减去几年
     *
     * @param date
     * @param year
     * @return
     */
    public  static Date year(Date date, int year) {
        java.util.Calendar Cal = java.util.Calendar.getInstance();
        Cal.setTime(date);
        Cal.add(java.util.Calendar.YEAR, year);
        return Cal.getTime();
    }

    /**
     * 在当前时间的基础上加或减去几月
     *
     * @param month
     * @return
     */
    public  static Date month(int month) {
        java.util.Calendar Cal = java.util.Calendar.getInstance();
        Cal.setTime(new Date());
        Cal.add(Calendar.MONTH, month);
        return Cal.getTime();
    }

    /**
     * 在指定的时间上加或减去几月
     *
     * @param date
     * @param month
     * @return
     */
    public  static Date month(Date date, int month) {
        java.util.Calendar Cal = java.util.Calendar.getInstance();
        Cal.setTime(date);
        Cal.add(java.util.Calendar.MONTH, month);
        return Cal.getTime();
    }

    /**
     * 在当前时间的基础上加或减去几天
     *
     * @param day
     * @return
     */
    public  static Date day(int day) {
        java.util.Calendar Cal = java.util.Calendar.getInstance();
        Cal.setTime(new Date());
        Cal.add(Calendar.DAY_OF_YEAR, day);
        return Cal.getTime();
    }

    /**
     * 在指定的时间上加或减去几天
     *
     * @param date
     * @param day
     * @return
     */
    public  static Date day(Date date, int day) {
        java.util.Calendar Cal = java.util.Calendar.getInstance();
        Cal.setTime(date);
        Cal.add(java.util.Calendar.DAY_OF_YEAR, day);
        return Cal.getTime();
    }

    /**
     * 在当前时间的基础上加或减去几小时-支持浮点数
     *
     * @param hour
     * @return
     */
    public  static Date hour(float hour) {
        java.util.Calendar Cal = java.util.Calendar.getInstance();
        Cal.setTime(new Date());
        Cal.add(java.util.Calendar.MINUTE, (int) (hour * 60));
        return Cal.getTime();
    }

    /**
     * 在制定的时间上加或减去几小时-支持浮点数
     *
     * @param date
     * @param hour
     * @return
     */
    public  static Date hour(Date date, float hour) {
        java.util.Calendar Cal = java.util.Calendar.getInstance();
        Cal.setTime(date);
        Cal.add(java.util.Calendar.MINUTE, (int) (hour * 60));
        return Cal.getTime();
    }

    /**
     * 在当前时间的基础上加或减去几分钟
     *
     * @param minute
     * @return
     */
    public  static Date minute(int minute) {
        java.util.Calendar Cal = java.util.Calendar.getInstance();
        Cal.setTime(new Date());
        Cal.add(java.util.Calendar.MINUTE, minute);
        return Cal.getTime();
    }

    /**
     * 在制定的时间上加或减去几分钟
     *
     * @param date
     * @param minute
     * @return
     */
    public  static Date minute(Date date, int minute) {
        java.util.Calendar Cal = java.util.Calendar.getInstance();
        Cal.setTime(date);
        Cal.add(java.util.Calendar.MINUTE, minute);
        return Cal.getTime();
    }


    /**
     * 判断字符串是否为日期字符串
     *
     * @param date 日期字符串
     * @return true or false
     */
    public  static boolean isDate(String date) {
        try {
            DateTimeInstance().parse(date);
            return true;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return false;
    }


    /**
     * 时间date1和date2的时间差-单位秒
     *
     * @param date1
     * @param date2
     * @return 秒
     */
    public  static long subtract(Date date1, Date date2) {
        long cha = (date2.getTime() - date1.getTime()) / 1000;
        return cha;
    }

    /**
     * 时间date1和date2的时间差-单位秒
     *
     * @param date1
     * @param date2
     * @return 秒
     */
    public  static long subtract(String date1, String date2) {
        long rs = 0;
        try {
            Date start = DateTimeInstance().parse(date1);
            Date end = DateTimeInstance().parse(date2);
            long cha = (end.getTime() - start.getTime()) / 1000;
            rs = cha;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return rs;
    }


    /**
     * 时间date1和date2的时间差 -单位分钟
     *
     * @param date1
     * @param date2
     * @return 分钟
     */
    public  static int subtractMinute(String date1, String date2) {
        int rs = 0;
        try {
            Date start = DateTimeInstance().parse(date1);
            Date end = DateTimeInstance().parse(date2);
            long cha = (end.getTime() - start.getTime()) / 1000;
            rs = (int) cha / (60);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return rs;
    }

    /**
     * 时间date1和date2的时间差-单位分钟
     *
     * @param date1
     * @param date2
     * @return 分钟
     */
    public  static int subtractMinute(Date date1, Date date2) {
        long cha = date2.getTime() - date1.getTime();
        return (int) cha / (1000 * 60);
    }

    /**
     * 时间date1和date2的时间差-单位小时
     *
     * @param date1
     * @param date2
     * @return 小时
     */
    public  static int subtractHour(Date date1, Date date2) {
        long cha = (date2.getTime() - date1.getTime()) / 1000;
        return (int) cha / (60 * 60);
    }

    /**
     * 时间date1和date2的时间差-单位小时
     *
     * @param date1
     * @param date2
     * @return 小时
     */
    public  static int subtractHour(String date1, String date2) {
        int rs = 0;
        try {
            Date start = DateTimeInstance().parse(date1);
            Date end = DateTimeInstance().parse(date2);
            long cha = (end.getTime() - start.getTime()) / 1000;
            rs = (int) cha / (60 * 60);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return rs;
    }


    /**
     * 时间date1和date2的时间差-单位天
     *
     * @param date1
     * @param date2
     * @return 天
     */
    public  static int subtractDay(String date1, String date2) {
        int rs = 0;
        try {
            Date start = DateTimeInstance().parse(date1);
            Date end = DateTimeInstance().parse(date2);
            long sss = (end.getTime() - start.getTime()) / 1000;
            rs = (int) sss / (60 * 60 * 24);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return rs;
    }

    /**
     * 时间date1和date2的时间差-单位天
     *
     * @param date1
     * @param date2
     * @return 天
     */
    public  static int subtractDay(Date date1, Date date2) {
        long cha = date2.getTime() - date1.getTime();
        return (int) cha / (1000 * 60 * 60 * 24);
    }

    /**
     * 时间date1和date2的时间差-单位月
     *
     * @param date1
     * @param date2
     * @return 月
     */
    public  static int subtractMonth(String date1, String date2) {
        int result;
        Calendar c1 = Calendar.getInstance();
        Calendar c2 = Calendar.getInstance();
        try {
            c1.setTime(DateInstance().parse(date1));
            c2.setTime(DateInstance().parse(date2));
            int year1 = c1.get(Calendar.YEAR);
            int month1 = c1.get(Calendar.MONTH);
            int year2 = c2.get(Calendar.YEAR);
            int month2 = c2.get(Calendar.MONTH);
            if (year1 == year2) {
                result = month2 - month1;
            } else {
                result = 12 * (year2 - year1) + month2 - month1;
            }
        } catch (ParseException e) {
            e.printStackTrace();
            result = -1;
        }
        return result;
    }

    /**
     * 时间date1和date2的时间差-单位月
     *
     * @param date1
     * @param date2
     * @return 月
     */
    public  static int subtractMonth(Date date1, Date date2) {
        int result;
        Calendar c1 = Calendar.getInstance();
        Calendar c2 = Calendar.getInstance();
        c1.setTime(date1);
        c2.setTime(date2);
        int year1 = c1.get(Calendar.YEAR);
        int month1 = c1.get(Calendar.MONTH);
        int year2 = c2.get(Calendar.YEAR);
        int month2 = c2.get(Calendar.MONTH);
        if (year1 == year2) {
            result = month2 - month1;
        } else {
            result = 12 * (year2 - year1) + month2 - month1;
        }
        return result;
    }

    /**
     * 时间date1和date2的时间差-单位年
     *
     * @param date1
     * @param date2
     * @return 年
     */
    public  static int subtractYear(String date1, String date2) {
        int result;
        Calendar c1 = Calendar.getInstance();
        Calendar c2 = Calendar.getInstance();
        try {
            c1.setTime(DateInstance().parse(date1));
            c2.setTime(DateInstance().parse(date2));
            int year1 = c1.get(Calendar.YEAR);
            int year2 = c2.get(Calendar.YEAR);
            result = year2 - year1;
        } catch (ParseException e) {
            e.printStackTrace();
            result = -1;
        }
        return result;
    }

    /**
     * 时间date1和date2的时间差-单位年
     *
     * @param date1
     * @param date2
     * @return 年
     */
    public  static int subtractYear(Date date1, Date date2) {
        int result;
        Calendar c1 = Calendar.getInstance();
        Calendar c2 = Calendar.getInstance();
        c1.setTime(date1);
        c2.setTime(date2);
        int year1 = c1.get(Calendar.YEAR);
        int year2 = c2.get(Calendar.YEAR);
        result = year2 - year1;
        return result;
    }

    /**
     * 获取俩个时间的查结果用时秒表示
     *
     * @param date1
     * @param date2
     * @return 几小时:几分钟:几秒钟
     * @Summary:此处可以讲计算结果包装成一个结构体返回便于格式化
     */
    public  static String subtractTime(String date1, String date2) {
        String result = "";
        try {
            Date start = DateTimeInstance().parse(date1);
            Date end = DateTimeInstance().parse(date2);
            long sss = (end.getTime() - start.getTime()) / 1000;
            int hh = (int) sss / (60 * 60);
            int mm = (int) (sss - hh * 60 * 60) / (60);
            int ss = (int) (sss - hh * 60 * 60 - mm * 60);
            result = hh + ":" + mm + ":" + ss;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * 获取俩个时间的查结果用时秒表示
     *
     * @param date1
     * @param date2
     * @return 几天-几小时:几分钟:几秒钟
     * @Summary:此处可以讲计算结果包装成一个结构体返回便于格式化
     */
    public  static String subtractDate(String date1, String date2) {
        String result = "";
        try {
            Date start = DateTimeInstance().parse(date1);
            Date end = DateTimeInstance().parse(date2);
            long sss = (end.getTime() - start.getTime()) / 1000;
            int dd = (int) sss / (60 * 60 * 24);
            int hh = (int) (sss - dd * 60 * 60 * 24) / (60 * 60);
            int mm = (int) (sss - dd * 60 * 60 * 24 - hh * 60 * 60) / (60);
            int ss = (int) (sss - dd * 60 * 60 * 24 - hh * 60 * 60 - mm * 60);
            result = dd + "-" + hh + ":" + mm + ":" + ss;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * 获取俩个时间之前的相隔的天数
     *
     * @param startTime
     * @param endTime
     * @return
     * @throws ParseException
     */
    public  static int subDay(Date startTime, Date endTime) {
        int days = 0;
        Calendar can1 = Calendar.getInstance();
        can1.setTime(startTime);
        Calendar can2 = Calendar.getInstance();
        can2.setTime(endTime);
        int year1 = can1.get(Calendar.YEAR);
        int year2 = can2.get(Calendar.YEAR);

        Calendar can = null;
        if (can1.before(can2)) {
            days -= can1.get(Calendar.DAY_OF_YEAR);
            days += can2.get(Calendar.DAY_OF_YEAR);
            can = can1;
        } else {
            days -= can2.get(Calendar.DAY_OF_YEAR);
            days += can1.get(Calendar.DAY_OF_YEAR);
            can = can2;
        }
        for (int i = 0; i < Math.abs(year2 - year1); i++) {
            days += can.getActualMaximum(Calendar.DAY_OF_YEAR);
            can.add(Calendar.YEAR, 1);
        }

        return days;
    }

    /**
     * 获取俩个时间之前的相隔的天数
     *
     * @param startTime
     * @param endTime
     * @return
     * @throws ParseException
     */
    public  static int subDay(String startTime, String endTime) {
        int days = 0;
        try {
            Date date1 = DateInstance().parse(DateInstance().format(DateTimeInstance().parse(startTime)));
            Date date2 = DateInstance().parse(DateInstance().format(DateTimeInstance().parse(endTime)));
            Calendar can1 = Calendar.getInstance();
            can1.setTime(date1);
            Calendar can2 = Calendar.getInstance();
            can2.setTime(date2);
            int year1 = can1.get(Calendar.YEAR);
            int year2 = can2.get(Calendar.YEAR);

            Calendar can = null;
            if (can1.before(can2)) {
                days -= can1.get(Calendar.DAY_OF_YEAR);
                days += can2.get(Calendar.DAY_OF_YEAR);
                can = can1;
            } else {
                days -= can2.get(Calendar.DAY_OF_YEAR);
                days += can1.get(Calendar.DAY_OF_YEAR);
                can = can2;
            }
            for (int i = 0; i < Math.abs(year2 - year1); i++) {
                days += can.getActualMaximum(Calendar.DAY_OF_YEAR);
                can.add(Calendar.YEAR, 1);
            }

        } catch (ParseException e) {
            e.printStackTrace();
        }
        return days;
    }

    /**
     * 返回俩个时间在时间段(例如每天的08:00-18:00)的时长-单位秒
     *
     * @param startDate
     * @param endDate
     * @param timeBurst 只就按该时间段内的08:00-18:00时长
     * @return 计算后的秒数
     * @throws ParseException
     * @summary 格式错误返回0
     */
    public  static long subtimeBurst(String startDate, String endDate, String timeBurst)
            throws ParseException {
        Date start = DateTimeInstance().parse(startDate);
        Date end = DateTimeInstance().parse(endDate);
        return subtimeBurst(start, end, timeBurst);
    }

    /**
     * 返回俩个时间在时间段(例如每天的08:00-18:00)的时长-单位秒
     *
     * @param startDate
     * @param endDate
     * @param timeBurst 只就按该时间段内的08:00-18:00时长
     * @return 计算后的秒数
     * @throws ParseException
     */
    public  static long subtimeBurst(Date startDate, Date endDate, String timeBurst)
            throws ParseException {
        long second = 0;
        Pattern p = Pattern.compile("^\\d{2}:\\d{2}-\\d{2}:\\d{2}");
        Matcher m = p.matcher(timeBurst);
        boolean falg = false;
        if (startDate.after(endDate)) {
            Date temp = startDate;
            startDate = endDate;
            endDate = temp;
            falg = true;
        }
        if (m.matches()) {
            String[] a = timeBurst.split("-");
            int day = subDay(startDate, endDate);
            if (day > 0) {
                long firstMintues = 0;
                long lastMintues = 0;
                long daySecond = 0;
                String strDayStart = DateInstance().format(startDate) + " " + a[0] + ":00";
                String strDayEnd = DateInstance().format(startDate) + " " + a[1] + ":00";
                Date dayStart = DateTimeInstance().parse(strDayStart);
                Date dayEnd = DateTimeInstance().parse(strDayEnd);
                daySecond = subtract(dayStart, dayEnd);
                if ((startDate.after(dayStart) || startDate.equals(dayStart))
                        && startDate.before(dayEnd)) {
                    firstMintues = (dayEnd.getTime() - startDate.getTime()) / 1000;
                } else if (startDate.before(dayStart)) {
                    firstMintues = (dayEnd.getTime() - dayStart.getTime()) / 1000;
                }
                dayStart = DateTimeInstance().parse(DateInstance().format(endDate) + " " + a[0] + ":00");
                dayEnd = DateTimeInstance().parse(DateInstance().format(endDate) + " " + a[1] + ":00");
                if (endDate.after(dayStart) && (endDate.before(dayEnd) || endDate.equals(dayEnd))) {
                    lastMintues = (endDate.getTime() - dayStart.getTime()) / 1000;
                } else if (endDate.after(dayEnd)) {
                    lastMintues = (dayEnd.getTime() - dayStart.getTime()) / 1000;
                }
                //第一天的秒数 + 最好一天的秒数 + 天数*全天的秒数
                second = firstMintues + lastMintues;
                second += (day - 1) * daySecond;
            } else {
                String strDayStart = DateInstance().format(startDate) + " " + a[0] + ":00";
                String strDayEnd = DateInstance().format(startDate) + " " + a[1] + ":00";
                Date dayStart = DateTimeInstance().parse(strDayStart);
                Date dayEnd = DateTimeInstance().parse(strDayEnd);
                if ((startDate.after(dayStart) || startDate.equals(dayStart))
                        && startDate.before(dayEnd) && endDate.after(dayStart)
                        && (endDate.before(dayEnd) || endDate.equals(dayEnd))) {
                    second = (endDate.getTime() - startDate.getTime()) / 1000;
                } else {
                    if (startDate.before(dayStart)) {
                        if (endDate.before(dayEnd)) {
                            second = (endDate.getTime() - dayStart.getTime()) / 1000;
                        } else {
                            second = (dayEnd.getTime() - dayStart.getTime()) / 1000;
                        }
                    }
                    if (startDate.after(dayStart)) {
                        if (endDate.before(dayEnd)) {
                            second = (endDate.getTime() - startDate.getTime()) / 1000;
                        } else {
                            second = (dayEnd.getTime() - startDate.getTime()) / 1000;
                        }
                    }
                }
                if ((startDate.before(dayStart) && endDate.before(dayStart))
                        || startDate.after(dayEnd) && endDate.after(dayEnd)) {
                    second = 0;
                }
            }
        } else {
            second = (endDate.getTime() - startDate.getTime()) / 1000;
        }
        if (falg) {
            second = Long.parseLong("-" + second);
        }
        return second;
    }

    /**
     * 时间Date在时间段(例如每天的08:00-18:00)上增加或减去second秒
     *
     * @param date
     * @param second
     * @param timeBurst
     * @return 计算后的时间
     * @Suumary 指定的格式错误后返回原数据
     */
    public  static Date calculate(String date, int second, String timeBurst) {
        Date start = null;
        try {
            start = DateTimeInstance().parse(date);
            return calculate(start, second, timeBurst);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return new Date();
    }

    /**
     * 时间Date在时间段(例如每天的08:00-18:00)上增加或减去second秒
     *
     * @param date
     * @param second
     * @param timeBurst
     * @return 计算后的时间
     * @Suumary 指定的格式错误后返回原数据
     */
    public  static Date calculate(Date date, int second, String timeBurst) {
        Pattern p = Pattern.compile("^\\d{2}:\\d{2}-\\d{2}:\\d{2}");
        Matcher m = p.matcher(timeBurst);
        java.util.Calendar cal = java.util.Calendar.getInstance();
        if (m.matches()) {
            String[] a = timeBurst.split("-");
            try {
                Date dayStart = DateTimeInstance().parse(DateInstance().format(date) + " " + a[0] + ":00");
                Date dayEnd = DateTimeInstance().parse(DateInstance().format(date) + " " + a[1] + ":00");
                int DaySecond = (int) subtract(dayStart, dayEnd);
                int toDaySecond = (int) subtract(dayStart, dayEnd);
                if (second >= 0) {
                    if ((date.after(dayStart) || date.equals(dayStart))
                            && (date.before(dayEnd) || date.equals(dayEnd))) {
                        cal.setTime(date);
                        toDaySecond = (int) subtract(date, dayEnd);
                    }
                    if (date.before(dayStart)) {
                        cal.setTime(dayStart);
                        toDaySecond = (int) subtract(dayStart, dayEnd);
                    }
                    if (date.after(dayEnd)) {
                        cal.setTime(day(dayStart, 1));
                        toDaySecond = 0;
                    }

                    if (second > toDaySecond) {
                        int day = (second - toDaySecond) / DaySecond;
                        int remainder = (second - toDaySecond) % DaySecond;
                        cal.setTime(day(dayStart, 1));
                        cal.add(Calendar.DAY_OF_YEAR, day);
                        cal.add(Calendar.SECOND, remainder);
                    } else {
                        cal.add(Calendar.SECOND, second);
                    }

                } else {
                    if ((date.after(dayStart) || date.equals(dayStart))
                            && (date.before(dayEnd) || date.equals(dayEnd))) {
                        cal.setTime(date);
                        toDaySecond = (int) subtract(date, dayStart);
                    }
                    if (date.before(dayStart)) {
                        cal.setTime(day(dayEnd, -1));
                        toDaySecond = 0;
                    }
                    if (date.after(dayEnd)) {
                        cal.setTime(dayEnd);
                        toDaySecond = (int) subtract(dayStart, dayEnd);
                    }
                    if (Math.abs(second) > Math.abs(toDaySecond)) {
                        int day = (Math.abs(second) - Math.abs(toDaySecond)) / DaySecond;
                        int remainder = (Math.abs(second) - Math.abs(toDaySecond)) % DaySecond;
                        cal.setTime(day(dayEnd, -1));
                        cal.add(Calendar.DAY_OF_YEAR, Integer.valueOf("-" + day));
                        cal.add(Calendar.SECOND, Integer.valueOf("-" + remainder));
                    } else {
                        cal.add(Calendar.SECOND, second);
                    }
                }
            } catch (ParseException e) {
                e.printStackTrace();
            }
        } else {
            cal.setTime(date);
        }
        return cal.getTime();
    }

    /**
     * 判断是否在某个时间段内
     * @param startTime
     * @param endTime
     * @param date
     * @return
     * @throws ParseException
     */
    public static boolean between(String startTime,String endTime,Date date)
            throws ParseException {
        return between(dateTime(startTime),dateTime(endTime),date);
    }

    /**
     * 判断在某个时间内
     * @param startTime
     * @param endTime
     * @param date
     * @return
     */
    public static boolean between(Date startTime,Date endTime,Date date){
        return date.after(startTime) && date.before(endTime);
    }
    
    
    /** 
     * 时间加减 
     */ 
    public static Date getDateAfterOpTime(Date date,int field,int amount) { 
        Calendar cal = Calendar.getInstance(); 
        cal.setTime(date); 
        cal.add(field,amount); 
        return cal.getTime(); 
    } 

    public static String dateToStr(Date date,String pattern){ 
        SimpleDateFormat sdf = new SimpleDateFormat(pattern); 
        return sdf.format(date); 
    } 

    public static Date strToDate(String dateStr,String pattern){ 
        SimpleDateFormat sdf = new SimpleDateFormat(pattern); 
        try { 
            return sdf.parse(dateStr); 
        } catch (ParseException e) { 
            e.printStackTrace(); 
        } 
        return null; 
    } 

    public static String transferLongToDate(Long millSec,String pattern) { 
        SimpleDateFormat sdf = new SimpleDateFormat(pattern); 
        Date date = new Date(millSec.longValue()); 
        return sdf.format(date); 
    } 

public static String getWeekDay() { 
return null; 
} 

public static Date getDate(String dateString, String format) { 
if (StringUtils.isNotEmpty(dateString)) { 
SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format); 
try { 
return simpleDateFormat.parse(dateString); 
} catch (ParseException e) { 
e.printStackTrace(); 
} 
} 
return null; 
} 

public static String getDate(Date date, String format) { 
if (null != date) { 
SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format); 
return simpleDateFormat.format(date); 
} 
return null; 
} 

    /** 
     * 获取指定时间的年份 
     * @param date 
     * @return 
     */ 
    public static int getYear(Date date){ 
        SimpleDateFormat sd = new SimpleDateFormat(DATE_FORMAT); 
        String dateStr = sd.format(date); 

        return Integer.parseInt(dateStr.substring(0, 4)); 

    } 

    /** 
     * 获取制定时间的月份 
     * @param date 
     * @return 
     */ 
    public static int getMonth(Date date){ 
        Calendar calendar = Calendar.getInstance(); 
        calendar.setTime(date); 
        return calendar.get(Calendar.MONTH) + 1; 

    } 

    /** 
     * 获取制定时间的日 
     * @param date 
     * @return 
     */ 
    public static int getDay(Date date){ 
        Calendar calendar = Calendar.getInstance(); 
        calendar.setTime(date); 
        return calendar.get(Calendar.DAY_OF_MONTH); 
    } 

    public static String getWeek(Date date){ 
        String[] weekDays = {"星期日", "星期一", "星期二", "星期三", "星期四", "星期五", "星期六"}; 
        Calendar calendar = Calendar.getInstance(); 
        calendar.setTime(date); 
        int w = calendar.get(Calendar.DAY_OF_WEEK) - 1; 
        if (w < 0) 
            w = 0; 
        return weekDays[w]; 
    } 

    //2015年7月5日 星期三 
    public static String getStrDateAndWeed(Date date){ 
        String str = getYear(date) + "年" + getMonth(date) + "月" + getDay(date) + "日" + " " + getWeek(date); 
        return str; 
    } 

    /** 
     * 计算两个日期相差的天数 
     * @param data1 
     * @param data2 
     * @return 
     */ 
    public static Integer days(String data1, String data2){ 
        SimpleDateFormat sf = new SimpleDateFormat(DATE_FORMAT); 
        Long c = null; 
        try { 
            c = sf.parse(data2).getTime()-sf.parse(data1).getTime(); 
        } catch (ParseException e) { 
            e.printStackTrace(); 
        } 
        long d = c/1000/60/60/24;//天 
        int days = (int)d; 
        return days; 
    } 

    /** 
     * 日期增长 
     * @param time 
     * @param dayCount 增长天数 
     * @return 
     */ 
    public static String time(String time, int dayCount){ 
        SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT); 
        String str=""; 
        try { 
            Date date = sdf.parse(time); 
            Calendar calendar = new GregorianCalendar(); 
            calendar.setTime(date); 
            calendar.add(Calendar.DATE, dayCount);// 
            //把日期往后增加一天.整数往后推,负数往前移动 
            date = calendar.getTime(); // 这个时间就是日期往后推一天的结果 
            str= sdf.format(date); 
        } catch (ParseException e) { 
            e.printStackTrace(); 
        } 
        return str; 
    } 
    
    
    
    
    
    @SuppressWarnings("finally")
    public static String FormatDate(String dateStr){
    
      HashMap<String, String> dateRegFormat = new HashMap<String, String>();
      dateRegFormat.put(
          "^\\d{4}\\D+\\d{1,2}\\D+\\d{1,2}\\D+\\d{1,2}\\D+\\d{1,2}\\D+\\d{1,2}\\D*$",
          "yyyy-MM-dd-HH-mm-ss");//2014年3月12日 13时5分34秒，2014-03-12 12:05:34，2014/3/12 12:5:34
      dateRegFormat.put("^\\d{4}\\D+\\d{2}\\D+\\d{2}\\D+\\d{2}\\D+\\d{2}$",
          "yyyy-MM-dd-HH-mm");//2014-03-12 12:05
      dateRegFormat.put("^\\d{4}\\D+\\d{2}\\D+\\d{2}\\D+\\d{2}$",
          "yyyy-MM-dd-HH");//2014-03-12 12
      dateRegFormat.put("^\\d{4}\\D+\\d{2}\\D+\\d{2}$", "yyyy-MM-dd");//2014-03-12
      dateRegFormat.put("^\\d{4}\\D+\\d{2}$", "yyyy-MM");//2014-03
      dateRegFormat.put("^\\d{4}$", "yyyy");//2014
      dateRegFormat.put("^\\d{14}$", "yyyyMMddHHmmss");//20140312120534
      dateRegFormat.put("^\\d{12}$", "yyyyMMddHHmm");//201403121205
      dateRegFormat.put("^\\d{10}$", "yyyyMMddHH");//2014031212
      dateRegFormat.put("^\\d{8}$", "yyyyMMdd");//20140312
      dateRegFormat.put("^\\d{6}$", "yyyyMM");//201403
      dateRegFormat.put("^\\d{2}\\s*:\\s*\\d{2}\\s*:\\s*\\d{2}$",
          "yyyy-MM-dd-HH-mm-ss");//13:05:34 拼接当前日期
      dateRegFormat.put("^\\d{2}\\s*:\\s*\\d{2}$", "yyyy-MM-dd-HH-mm");//13:05 拼接当前日期
      dateRegFormat.put("^\\d{2}\\D+\\d{1,2}\\D+\\d{1,2}$", "yy-MM-dd");//14.10.18(年.月.日)
      dateRegFormat.put("^\\d{1,2}\\D+\\d{1,2}$", "yyyy-dd-MM");//30.12(日.月) 拼接当前年份
      dateRegFormat.put("^\\d{1,2}\\D+\\d{1,2}\\D+\\d{4}$", "dd-MM-yyyy");//12.21.2013(日.月.年)
    
      String curDate =new SimpleDateFormat("yyyy-MM-dd").format(new Date());
      DateFormat formatter1 =new SimpleDateFormat(DATETIME_FORMAT);
      DateFormat formatter2;
      String dateReplace;
      String strSuccess="";
      try {
        for (String key : dateRegFormat.keySet()) {
          if (Pattern.compile(key).matcher(dateStr).matches()) {
            formatter2 = new SimpleDateFormat(dateRegFormat.get(key));
            if (key.equals("^\\d{2}\\s*:\\s*\\d{2}\\s*:\\s*\\d{2}$")
                || key.equals("^\\d{2}\\s*:\\s*\\d{2}$")) {//13:05:34 或 13:05 拼接当前日期
              dateStr = curDate + "-" + dateStr;
            } else if (key.equals("^\\d{1,2}\\D+\\d{1,2}$")) {//21.1 (日.月) 拼接当前年份
              dateStr = curDate.substring(0, 4) + "-" + dateStr;
            }
            dateReplace = dateStr.replaceAll("\\D+", "-");
            // System.out.println(dateRegExpArr[i]);
            strSuccess = formatter1.format(formatter2.parse(dateReplace));
            break;
          }
        }
      } catch (Exception e) {
        System.err.println("-----------------日期格式无效:"+dateStr);
        throw new Exception( "日期格式无效");
      } finally {
        return strSuccess;
      }
    }
    
    
    

        public static boolean isDateStr(String dateStr)
        {
            /**
             * 判断日期格式和范围
             */
           // String rexp = "^((\\d{2}(([02468][048])|([13579][26]))[\\-\\/\\s]?((((0?[13578])|(1[02]))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(3[01])))|(((0?[469])|(11))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(30)))|(0?2[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])))))|(\\d{2}(([02468][1235679])|([13579][01345789]))[\\-\\/\\s]?((((0?[13578])|(1[02]))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(3[01])))|(((0?[469])|(11))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(30)))|(0?2[\\-\\/\\s]?((0?[1-9])|(1[0-9])|(2[0-8]))))))";
      	  String rexp = "((^((1[8-9]\\d{2})|([2-9]\\d{3}))([-\\/\\._])(10|12|0?[13578])([-\\/\\._])(3[01]|[12][0-9]|0?[1-9])$)|(^((1[8-9]\\d{2})|([2-9]\\d{3}))([-\\/\\._])(11|0?[469])([-\\/\\._])(30|[12][0-9]|0?[1-9])$)|(^((1[8-9]\\d{2})|([2-9]\\d{3}))([-\\/\\._])(0?2)([-\\/\\._])(2[0-8]|1[0-9]|0?[1-9])$)|(^([2468][048]00)([-\\/\\._])(0?2)([-\\/\\._])(29)$)|(^([3579][26]00)([-\\/\\._])(0?2)([-\\/\\._])(29)$)|(^([1][89][0][48])([-\\/\\._])(0?2)([-\\/\\._])(29)$)|(^([2-9][0-9][0][48])([-\\/\\._])(0?2)([-\\/\\._])(29)$)|(^([1][89][2468][048])([-\\/\\._])(0?2)([-\\/\\._])(29)$)|(^([2-9][0-9][2468][048])([-\\/\\._])(0?2)([-\\/\\._])(29)$)|(^([1][89][13579][26])([-\\/\\._])(0?2)([-\\/\\._])(29)$)|(^([2-9][0-9][13579][26])([-\\/\\._])(0?2)([-\\/\\._])(29)$))";
      			  Pattern pat = Pattern.compile(rexp);  
             
            Matcher mat = pat.matcher(dateStr);  
             
            boolean dateType = mat.matches();

            return dateType;
        }
    
        
        
        public static boolean isDateTime(String dateTimeStr)
        {
            /**
             * 判断日期格式和范围
             */
           // String rexp = "^((\\d{2}(([02468][048])|([13579][26]))[\\-\\/\\s]?((((0?[13578])|(1[02]))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(3[01])))|(((0?[469])|(11))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(30)))|(0?2[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])))))|(\\d{2}(([02468][1235679])|([13579][01345789]))[\\-\\/\\s]?((((0?[13578])|(1[02]))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(3[01])))|(((0?[469])|(11))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(30)))|(0?2[\\-\\/\\s]?((0?[1-9])|(1[0-9])|(2[0-8]))))))";
      	  String rexp= "^((\\d{2}(([02468][048])|([13579][26]))[\\-\\/\\s]?((((0?[13578])|(1[02]))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(3[01])))|(((0?[469])|(11))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(30)))|(0?2[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])))))|(\\d{2}(([02468][1235679])|([13579][01345789]))[\\-\\/\\s]?((((0?[13578])|(1[02]))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(3[01])))|(((0?[469])|(11))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(30)))|(0?2[\\-\\/\\s]?((0?[1-9])|(1[0-9])|(2[0-8]))))))(\\s(((0?[0-9])|([1-2][0-9]))\\:([0-5]?[0-9])((\\s)|(\\:([0-5]?[0-9])))))?$";
      	  Pattern pat = Pattern.compile(rexp);  
             
            Matcher mat = pat.matcher(dateTimeStr);  
             
            boolean dateType = mat.matches();

            return dateType;
        }
        
        
        public static boolean isMaybeDate(String dateStr)
        {
      	  String rexp="\\d{4}\\D+\\d{2}\\D+\\d{2}$";
      	  Pattern pat = Pattern.compile(rexp);  
            
            Matcher mat = pat.matcher(dateStr);  
             
            boolean dateType = mat.matches();
            
            String rexp2="\\d{4}\\/\\d{2}\\/\\d{2}$";
      	  Pattern pat2 = Pattern.compile(rexp2);  
            
            Matcher mat2 = pat2.matcher(dateStr);  
             
            boolean dateType2 = mat2.matches();
            
            return dateType || dateType2;
        }
        
    
        public static boolean isMaybeDateTime(String dateTimeStr)
        {
      	  String rexp="^\\d{4}\\D+\\d{1,2}\\D+\\d{1,2}\\D+\\d{1,2}\\D+\\d{1,2}\\D+\\d{1,2}\\D*$";
      	  Pattern pat = Pattern.compile(rexp);  
            
            Matcher mat = pat.matcher(dateTimeStr);  
             
            boolean dateType = mat.matches();
            
            String rexp2="^\\d{4}\\/\\d{1,2}\\/\\d{1,2}\\D+\\d{1,2}\\D+\\d{1,2}\\D+\\d{1,2}\\D*$";
      	  Pattern pat2 = Pattern.compile(rexp2);  
            
            Matcher mat2 = pat2.matcher(dateTimeStr);  
             
            boolean dateType2 = mat2.matches();
            
            return dateType || dateType2;
        }
        
        public static boolean isMaybeHaveDateTime(String str)
        {
      	  boolean dateType = false;
      	  boolean dateType2 = false;
      	  String rexp="\\d{4}\\-\\d{1,2}\\-\\d{1,2}\\s+\\d{1,2}\\:\\d{1,2}\\:\\d{1,2}";
      	  Pattern p = Pattern.compile(rexp);
            Matcher m = p.matcher(str);
            if (m.find()) {
          	  dateType=true;
            }
            
            String rexp2="\\d{4}\\/\\d{1,2}\\/\\d{1,2}\\s+\\d{1,2}\\:\\d{1,2}\\:\\d{1,2}";
      	  Pattern p2 = Pattern.compile(rexp2);
            Matcher m2 = p2.matcher(str);
            if (m2.find()) {
          	  dateType2=true;
            }
            
            return dateType || dateType2;
        }
        
        
        
        
        public static boolean isMaybeHaveDateTime1(String str)
        {
      	  boolean dateType = false;
      	
      	  String rexp="\\d{4}\\-\\d{1,2}\\-\\d{1,2}\\s+\\d{1,2}\\:\\d{1,2}\\:\\d{1,2}";
      	  Pattern p = Pattern.compile(rexp);
            Matcher m = p.matcher(str);
            if (m.find()) {
          	  dateType=true;
            }
          
            
            return dateType ;
        }
        
        
        public static boolean isMaybeHaveDateTime2(String str)
        {
      	 
      	  boolean dateType2 = false;
            String rexp2="\\d{4}\\/\\d{1,2}\\/\\d{1,2}\\s+\\d{1,2}\\:\\d{1,2}\\:\\d{1,2}";
      	  Pattern p2 = Pattern.compile(rexp2);
            Matcher m2 = p2.matcher(str);
            if (m2.find()) {
          	  //System.out.println(m2.group(0));
          	  dateType2=true;
            }
            
            return  dateType2;
        }
        
        
        
        
        public static boolean isMaybeHaveDate(String str)
        {
      	  boolean dateType = false;
      	  boolean dateType2 = false;
      	  String rexp="\\d{4}\\D+\\d{2}\\D+\\d{2}$";
      	  Pattern pat = Pattern.compile(rexp);  
            
            Matcher mat = pat.matcher(str);  
            if (mat.find()) {
          	  dateType=true;
            }
            
            String rexp2="\\d{4}\\/\\d{2}\\/\\d{2}$";
      	  Pattern pat2 = Pattern.compile(rexp2);  
            Matcher mat2 = pat2.matcher(str);  
             
            if (mat2.find()) {
          	  dateType=true;
            }
            
            return dateType || dateType2;
        }
        
        
    
    public static void main(String[] args) {
    	/**
      String[] dateStrArray=new String[]{
          "2014-03-12 12:05:34",
          "2014-03-12 12:05",
          "2014-03-12 12",
          "2014-03-12",
          "2014-03",
          "2014",
          "20140312120534",
          "2014/03/12 12:05:34",
          "2014/3/12 12:5:34",
          "2014年3月12日 13时5分34秒",
          "201403121205",
          "1234567890",
          "20140312",
          "201403",
          "2000 13 33 13 13 13",
          "30.12.2013",
          "12.21.2013",
          "21.1",
          "13:05:34",
          "12:05",
          "14.1.8",
          "14.10.18"
      };
      for(int i=0;i<dateStrArray.length;i++){
        System.out.println(dateStrArray[i] +"------------------------------".substring(1,30-dateStrArray[i].length())+ FormatDate(dateStrArray[i]));
      }
        
      System.out.println(FormatDate( "2014/03/12 12:05:34"));
      
      System.out.println(isMaybeHaveDateTime2("wewew0001/01/01 08:00:0021212"));
      
      
      String re = "\\d{4}\\-\\d{1,2}\\-\\d{1,2}\\s+\\d{1,2}\\:\\d{1,2}\\:\\d{1,2}";
      String str = "wewew0001-01-01 08:00:0021212\"1001/01/0108:00:00\"";
      Pattern p = Pattern.compile(re);
      Matcher m = p.matcher(str);
      while(m.find()){
      System.out.println(m.group(0));
  
      }
      **/
    	
    	
    	
    	System.out.println(dateStr_UTC_Z(new Date()));
    	//2018-07-25T07:26:34Z
    	System.out.println(dateStr_UTC_Z_ALL(new Date()));
    	//2018-07-25T07:26:34.680Z
    	System.out.println(dateStr_UTC_08(new Date()));
    	//2018-07-25T15:27:45.564+08:00
    	System.out.println(dateStr_UTC2CST("2018-07-25T15:32:27.367+08:00"));
    	System.out.println(dateStr_UTC2CST("2018-07-25T07:26:34.680Z"));
    	
    	
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		// CST(北京时间)在东8区
		sdf.setTimeZone(TimeZone.getTimeZone("GMT+8"));
		System.out.println(sdf.format(new Date()));
		
			try {
				System.out.println(dateStr_UTC_Z(sdf.parse("2018-07-25 16:17:39")));
			} catch (ParseException e) {
				e.printStackTrace();
			}
    }
}
