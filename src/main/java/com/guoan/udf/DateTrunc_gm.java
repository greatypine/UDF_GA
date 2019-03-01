package com.guoan.udf;

import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
  * Description:  �Զ��庯��,�����ǻ�ȡָ��ʱ��ε���С��һ�� 
  * @author lyy  
  * @date 2018��7��9��
 */
public class DateTrunc_gm  extends UDF{

	/**
	  * Title: evaluate
	  * Description: 
	  * @param dateType 'week' or 'month' or 'day'
	  * @param dateNum ��ǰ�ƶ೤ʱ��       Ĭ��Ϊ 1 
	  * @param time ����ʱ��  Ĭ��Ϊ ��ǰʱ��
	  * @return yyyy-MM-dd ���͵�ʱ��
	 */
	public String evaluate(String dateType , Integer dateNum , String time){
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		SimpleDateFormat returnSdf = new SimpleDateFormat("yyyy-MM-dd");
		
		//�п�
		if(dateType == null && "".equals(dateType) ){
			return "DATETYPE_IS_NOT_NULL";
		}
		if(dateNum == null){
			dateNum =1;
		}
		//��ȡʱ�����
		Calendar cal = Calendar.getInstance(); 
		if(time != null && !"".equals(time.trim())){
			//Ĭ��ȡ��ǰʱ��,�в��������ȡ��������ʱ��
			try{
				cal.setTime(sdf.parse(time.trim()));
			}catch(Exception e){
				e.printStackTrace();
				return "time_converter_error";
			}
		}
		//Ҫ��dateType��Сд����ת��
		dateType = dateType.toLowerCase().trim();
		
		if("week".equals(dateType)){
			//��ǰ��  (dateNum -1 )* 7 + (��ǰ����-1) ��
			//��ȡ��ǰ�����ڼ�
			int wi = cal.get(Calendar.DAY_OF_WEEK) - 1;
			cal.add(Calendar.DAY_OF_YEAR, -((dateNum -1 )*7 +(wi-1)));
			String returnDate = returnSdf.format(cal.getTime());
			return returnDate;
			
		}else if("month".equals(dateType)){
			//��ǰ�� (dateNum -1) ����,Ȼ��ȡ���µ�һ��
			cal.add(Calendar.MONTH, -(dateNum -1));
			cal.set(Calendar.DAY_OF_MONTH,1);
			String returnDate = returnSdf.format(cal.getTime());
			return returnDate;
			
		}else if("day".equals(dateType)){
			//��ǰ��(dateNum��)
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
