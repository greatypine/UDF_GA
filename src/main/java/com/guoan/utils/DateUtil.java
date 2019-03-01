package com.guoan.utils;

import java.time.Instant;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.util.Date;

/**
  * Description: 日期工具类  
  * @author lyy  
  * @date 2018年5月24日
 */
public class DateUtil {

	 /**
	   * Title: dateCompareTo_days
	   * Description:  获取日期相差 (天数)
	   * @param date1
	   * @param date2
	   * @return
	  */
	 public static int dateCompareTo_days(Date date1, Date date2) {
		    LocalDate localDate1 = date2LocalDate(date1);
		    LocalDate localDate2 = date2LocalDate(date2);
	    	Period period = Period.between(localDate1, localDate2);
	        return period.getDays();
	    }
	 
	 /**
	   * Title: date2LocalDate
	   * Description: date转换为java8的localdate
	   * @param date
	   * @return
	  */
	 public static LocalDate date2LocalDate(Date date){
		 
        Instant instant = date.toInstant();
        ZoneId zoneId = ZoneId.systemDefault();
        LocalDate localDate = instant.atZone(zoneId).toLocalDate();
        return localDate;
	 }
	 
}
