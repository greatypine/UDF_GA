package com.guoan.udf;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.guoan.utils.DateUtil;

/**
  * Description: �û��Զ����������ں���
  * @author lyy  
 */
public class DataTrunc_df  extends UDF{

	
	/**
	  * Title: evaluate
	  * Description: 
	  * @param time �ֶ�ʱ��
	  * @param dfTime �û��Զ�������ʱ��   yyyy-MM-dd 
	  * @param param  �û��Զ�������
	  * @param flag  ture ����,  false  ����
	  * @return ��������
	  * @throws Exception
	 */
	public String evaluate(String time ,String dfTime, int param ,boolean flag  ) {
		//����У��
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
			//���Ϊ����
			//�����������С���û���������
			if(sqlDate.getTime() < dfDate.getTime()){
				//����һ�����
				return "timeMin";
			}else{
				//ȡ�������ڵ�������
				int dayGap = DateUtil.dateCompareTo_days(dfDate,sqlDate);
				//�����������Ĳ���ȡ����
				int remainder =  dayGap%param;
				//��ǰ������ -1 ��, ������  param - ���� ��
				
				cal.add(Calendar.DAY_OF_YEAR, -(remainder));
				startTime = returnSdf.format(cal.getTime());
				//�ָ�ʱ��
				cal.add(Calendar.DAY_OF_YEAR, remainder);
				cal.add(Calendar.DAY_OF_YEAR, param-remainder-1);
				endTime = returnSdf.format(cal.getTime());
				return startTime+"-"+endTime;
			}
			
		}else{
			//����
			//����������ڴ����û���������
			if(sqlDate.getTime() > dfDate.getTime()){
				//����һ�����
				return "timeMax";
			}else{
				//ȡ�������ڵ�������
				int dayGap = DateUtil.dateCompareTo_days(sqlDate ,dfDate);
				//�����������Ĳ���ȡ����
				int remainder =  dayGap%param;
				//��ǰ��������(end)
				cal.add(Calendar.DAY_OF_YEAR, remainder);
				endTime = returnSdf.format(cal.getTime());
				//�ָ�ʱ��
				cal.add(Calendar.DAY_OF_YEAR, -remainder);
				//������  (���� - ���� -1 ��)(start) ����β������ͷ
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
