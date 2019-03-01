package com.guoan.udf;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.hive.ql.exec.UDF;
/**
  * Description:  自定义时间函数 
  * @author lyy  
  * @date 2018年5月23日
 */
public class DataTrunc_WM extends UDF {

	/**
	  * Title: evaluate
	  * Description: 
	  * @param time  传入表中的时间字段
	  * @param dataType 时间类型("week","month")
	  * @param param 传入的时间间隔
	  * @return 返回 时间所在的时间区域
	 * @throws Exception 
	 */
	public String evaluate(String time ,String dataType, int param)  {
		
		if(time == null || "NULL".equals(time) || "null".equals(time) ){
			return "NULLTIME";
		}

		
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		SimpleDateFormat returnSdf = new SimpleDateFormat("yyyyMMdd");
		
		Calendar cal = Calendar.getInstance(); 
		
		try {
			cal.setTime(sdf.parse(time));
		} catch (ParseException e) {
			//如果日期转换异常,就输出指定标识
			return "ERRORTIME";
		}
		
		String startTime ="";
		String endTime ="";
		
		if("week".equals(dataType.toLowerCase())){
			//如果为week,那么传入的是自定义区间 包含头不包含尾
			//比如传入为 3  那么就是本周三到下周二 是一个周区间
			if(param > 7 || param < 1){
				//在此退出
				return "week_only_1_7";
			}
			//获取当天是星期几
			int wi = cal.get(Calendar.DAY_OF_WEEK) - 1;
			if(wi < param){
				//小于的情况
				int differ = param-wi;
				//往前推 (差-1) end
				cal.add(Calendar.DAY_OF_YEAR, (differ-1));
				endTime = returnSdf.format(cal.getTime());
				//恢复
				cal.add(Calendar.DAY_OF_YEAR, -(differ-1));
				//往后推(7-差)start
				cal.add(Calendar.DAY_OF_YEAR, -(7-differ));
				startTime = returnSdf.format(cal.getTime());
			}else{
				//大于等于的情况
				int differ = wi-param;
				//往前推差 start
				cal.add(Calendar.DAY_OF_YEAR, -differ);
				startTime = returnSdf.format(cal.getTime());
				//恢复
				cal.add(Calendar.DAY_OF_YEAR, +differ);
				//往前推(7-cha-1)end
				cal.add(Calendar.DAY_OF_YEAR, +(7-differ-1));
				endTime = returnSdf.format(cal.getTime());
			}
			
			return startTime+"-"+endTime;
			
		}else if ("month".equals(dataType.toLowerCase())){
			
			if(param > 31 || param < 1){
				//在此退出
				return "month_only_1-31";
			}
			
			if(param == 1){
				
				//输入 1 就是以月为单位
				cal.set(Calendar.DAY_OF_MONTH,1);//本月第一天
				startTime = returnSdf.format(cal.getTime());
				
				cal.add(Calendar.MONTH,1);//本月最后一天
				cal.set(Calendar.DAY_OF_MONTH,0);
				endTime = returnSdf.format(cal.getTime());
				
				return startTime+"-"+endTime;
				
			}else{
				//输入非1的数就是按 月号为单位
				//获取当前日期
				int today = cal.get(Calendar.DAY_OF_MONTH);
				if(today>=param){
					//如果当前天数大于 输入 参数
					//拿到这个月的开始时间往后推一个月
					cal.add(Calendar.DAY_OF_YEAR, -(today-param));
					startTime = returnSdf.format(cal.getTime());
					cal.add(Calendar.MONTH, 1);
					//减去一天,不包含尾
					cal.add(Calendar.DAY_OF_YEAR, -1);
					endTime = returnSdf.format(cal.getTime());
					return startTime+"-"+endTime;
				}else{
					//拿到这个区域的结束时间往前推一个月
					cal.add(Calendar.DAY_OF_YEAR, (param-today-1));
					endTime = returnSdf.format(cal.getTime());
					cal.add(Calendar.MONTH, -1);
					//加一天
					cal.add(Calendar.DAY_OF_YEAR, 1);
					startTime = returnSdf.format(cal.getTime());
					return startTime+"-"+endTime;
				}
			}
		}else{
			return "datatype_only_support_week_or_month";
		}
		
	}
	
	
	public static void main(String[] args) throws Exception {
		
		DataTrunc_WM dw = new DataTrunc_WM();
		String evaluate = dw.evaluate("2018-05-03 18:00:24.0", "week", 3);
		System.out.println(evaluate);
		
	
		
	}
	
	
	
	
}
