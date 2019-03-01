package com.guoan.udf;

import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
  * Description:  自定义函数,作用是获取指定时间段的最小的一天 
  * @author lyy  
  * @date 2018年7月9日
 */
public class DateTrunc_gm  extends UDF{

	/**
	  * Title: evaluate
	  * Description: 
	  * @param dateType 'week' or 'month' or 'day'
	  * @param dateNum 往前推多长时间       默认为 1 
	  * @param time 输入时间  默认为 当前时间
	  * @return yyyy-MM-dd 类型的时间
	 */
	public String evaluate(String dateType , Integer dateNum , String time){
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		SimpleDateFormat returnSdf = new SimpleDateFormat("yyyy-MM-dd");
		
		//判空
		if(dateType == null && "".equals(dateType) ){
			return "DATETYPE_IS_NOT_NULL";
		}
		if(dateNum == null){
			dateNum =1;
		}
		//获取时间对象
		Calendar cal = Calendar.getInstance(); 
		if(time != null && !"".equals(time.trim())){
			//默认取当前时间,有参数输入就取参数输入时间
			try{
				cal.setTime(sdf.parse(time.trim()));
			}catch(Exception e){
				e.printStackTrace();
				return "time_converter_error";
			}
		}
		//要将dateType大小写进行转换
		dateType = dateType.toLowerCase().trim();
		
		if("week".equals(dateType)){
			//往前推  (dateNum -1 )* 7 + (当前星期-1) 天
			//获取当前是星期几
			int wi = cal.get(Calendar.DAY_OF_WEEK) - 1;
			cal.add(Calendar.DAY_OF_YEAR, -((dateNum -1 )*7 +(wi-1)));
			String returnDate = returnSdf.format(cal.getTime());
			return returnDate;
			
		}else if("month".equals(dateType)){
			//往前推 (dateNum -1) 个月,然后取当月第一天
			cal.add(Calendar.MONTH, -(dateNum -1));
			cal.set(Calendar.DAY_OF_MONTH,1);
			String returnDate = returnSdf.format(cal.getTime());
			return returnDate;
			
		}else if("day".equals(dateType)){
			//往前推(dateNum天)
			cal.add(Calendar.DAY_OF_YEAR, -dateNum);
			String returnDate = returnSdf.format(cal.getTime());
			return returnDate;
			
		}else{
			return "week_month_day";
		}
		
	}
	
	

	public static void main(String[] args) {
		DateTrunc_gm dg = new DateTrunc_gm();
		String time = "2018-07-06 11:11:11";
		
		String evaluate1 = dg.evaluate("week",null, null);
		System.out.println(evaluate1);
		
		
		String evaluate2 = dg.evaluate("month",3, time);
		System.out.println(evaluate2);
		
		String evaluate3 = dg.evaluate("day",null, null);
		System.out.println(evaluate3);
	}
	
}
