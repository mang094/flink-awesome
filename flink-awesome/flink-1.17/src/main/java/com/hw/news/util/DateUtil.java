package com.hw.news.util;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public final class DateUtil {

    private DateUtil() {

    }

    public static final String DATE_FORMAT_1 = "yyyy-MM-dd hh:mm:ss";

    public static final String DATE_FORMAT_2 = "yyyy-MM-dd hh:mm";

    public static final String DATE_FORMAT_3 = "yyyy-MM-dd";

    public static Date getStrToDateFormat(String date, String format) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
        try {
            return simpleDateFormat.parse(date);
        } catch (ParseException e) {
            System.err.println("Date parse error: ");
            System.err.println(date);
        }
        return null;
    }

    public static Date subtractDays(Date date, int days) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.DAY_OF_YEAR, -days);
        return calendar.getTime();
    }

    public static Timestamp getTimestamp(String dateStr, String format) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        try {
            Date parsedDate = dateFormat.parse(dateStr);
            return new Timestamp(parsedDate.getTime());
        } catch (ParseException e) {
            return null;
        }
    }

    public static Timestamp publishTime(String dateStr) {
        Timestamp timestamp = getTimestamp(dateStr, DATE_FORMAT_1);
        if (timestamp == null) {
            timestamp = getTimestamp(dateStr, DATE_FORMAT_2);
        }
        if (timestamp == null) {
            timestamp = getTimestamp(dateStr, DATE_FORMAT_3);
        }
        return timestamp;
    }

    public static boolean isToday(Date date) {
        // 创建 Calendar 对象
        Calendar calendar = Calendar.getInstance();
        // 设置 Calendar 对象为当前时间
        calendar.setTimeInMillis(System.currentTimeMillis());
        // 获取当前年、月、日
        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH) + 1; // 注意月份从0开始计数，所以需加1
        int dayOfMonth = calendar.get(Calendar.DAY_OF_MONTH);

        // 将 Date 转换成 Calendar 格式
        calendar.clear();
        calendar.setTime(date);
        // 获取今天的年、月、日
        int todayYear = calendar.get(Calendar.YEAR);
        int todayMonth = calendar.get(Calendar.MONTH) + 1; // 注意月份从0开始计数，所以需加1
        int todayDayOfMonth = calendar.get(Calendar.DAY_OF_MONTH);
        // 判断当前日期是否为今天
        if (year == todayYear && month == todayMonth && dayOfMonth == todayDayOfMonth) {
            return true;
        } else {
            return false;
        }
    }

}
