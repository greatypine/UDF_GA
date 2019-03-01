package com.guoan.udf;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.guoan.utils.DateUtil;

/**
  * Description: 用户自定义区域日期函数
  * @author lyy  
 */
public class DataTrunc_df  extends UDF{

	
	/**
	  * Title: evaluate
	  * Description: 
	  * @param time 字段时间
	  * @param dfTime 用户自定义起至时间   yyyy-MM-dd 
	  * @param param  用户自定义日期
	  * @param flag  ture 正推,  false  反推
	  * @return 日期区域
	  * @throws Exception
	 */
	public String evaluate(String time ,String dfTime, int param ,boolean flag  ) {
		//参数校验
		if(time == null || "NULL".equals(time) || "null".equals(time) ){
			return "NULLTIME";
		}
		
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		SimpleDateFormat dfTimeSdf = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat returnSdf = new SimpleDateFormat("yyyyMMdd");
		
		Calendar cal = Calendar.getInstance(); 
		Date sqlDate ;
		try {
			if(time.trim().length() >10){
				sqlDate = sdf.parse(time.trim());
			}else{
				sqlDate = dfTimeSdf.parse(time.trim());
			}
			
		} catch (ParseException e1) {
			return "ERRORTIME";
		}
		
		
		cal.setTime(sqlDate);
		Date dfDate;
		try {
			dfDate = dfTimeSdf.parse(dfTime);
		} catch (ParseException e) {
			return "timeformaterror";
		}
		
		String startTime ="";
		String endTime ="";
		
		if(flag){
			//如果为正推
			//如果输入日期小于用户定义日期
			if(sqlDate.getTime() < dfDate.getTime()){
				//返回一个标记
				return "timeMin";
			}else{
				//取两个日期的天数差
				int dayGap = DateUtil.dateCompareTo_days(dfDate,sqlDate);
				//天数差对输入的参数取余数
				int remainder =  dayGap%param;
				//往前推余数 -1 天, 往后推  param - 余数 天
				
				cal.add(Calendar.DAY_OF_YEAR, -(remainder));
				startTime = returnSdf.format(cal.getTime());
				//恢复时间
				cal.add(Calendar.DAY_OF_YEAR, remainder);
				cal.add(Calendar.DAY_OF_YEAR, param-remainder-1);
				endTime = returnSdf.format(cal.getTime());
				return startTime+"-"+endTime;
			}
			
		}else{
			//反推
			//如果输入日期大于用户定义日期
			if(sqlDate.getTime() > dfDate.getTime()){
				//返回一个标记
				return "timeMax";
			}else{
				//取两个日期的天数差
				int dayGap = DateUtil.dateCompareTo_days(sqlDate ,dfDate);
				//天数差对输入的参数取余数
				int remainder =  dayGap%param;
				//往前推余数天(end)
				cal.add(Calendar.DAY_OF_YEAR, remainder);
				endTime = returnSdf.format(cal.getTime());
				//恢复时间
				cal.add(Calendar.DAY_OF_YEAR, -remainder);
				//往后推  (参数 - 余数 -1 天)(start) 包含尾不包含头
				cal.add(Calendar.DAY_OF_YEAR, -(param-remainder-1));
				startTime = returnSdf.format(cal.getTime());
				return startTime+"-"+endTime;
			}
		}
		
	}
	
	
	public static void main(String[] args) throws Exception {
		DataTrunc_df dd = new DataTrunc_df();
		String evaluate = dd.evaluate("2018-07-02 07:47:21.0", "2018-01-23", 12, true);
		System.out.println(evaluate);//20180623-20180704
		String evaluate2 = dd.evaluate("2018-07-02", "2018-01-23", 12, true);
		System.out.println(evaluate2);//20180623-20180704
		String str = "2017-04-16";
		Calendar cal = Calendar.getInstance();
		SimpleDateFormat dfTimeSdf = new SimpleDateFormat("yyyy-MM-dd");
		cal.setTime(dfTimeSdf.parse(str));
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String format = sdf.format(cal.getTime());
		System.out.println(format);
		
	}
	
	
}
