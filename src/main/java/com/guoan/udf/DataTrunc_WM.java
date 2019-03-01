package com.guoan.udf;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.hive.ql.exec.UDF;
/**
  * Description:  �Զ���ʱ�亯�� 
  * @author lyy  
  * @date 2018��5��23��
 */
public class DataTrunc_WM extends UDF {

	/**
	  * Title: evaluate
	  * Description: 
	  * @param time  ������е�ʱ���ֶ�
	  * @param dataType ʱ������("week","month")
	  * @param param �����ʱ����
	  * @return ���� ʱ�����ڵ�ʱ������
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
			//�������ת���쳣,�����ָ����ʶ
			return "ERRORTIME";
		}
		
		String startTime ="";
		String endTime ="";
		
		if("week".equals(dataType.toLowerCase())){
			//���Ϊweek,��ô��������Զ������� ����ͷ������β
			//���紫��Ϊ 3  ��ô���Ǳ����������ܶ� ��һ��������
			if(param > 7 || param < 1){
				//�ڴ��˳�
				return "week_only_1_7";
			}
			//��ȡ���������ڼ�
			int wi = cal.get(Calendar.DAY_OF_WEEK) - 1;
			if(wi < param){
				//С�ڵ����
				int differ = param-wi;
				//��ǰ�� (��-1) end
				cal.add(Calendar.DAY_OF_YEAR, (differ-1));
				endTime = returnSdf.format(cal.getTime());
				//�ָ�
				cal.add(Calendar.DAY_OF_YEAR, -(differ-1));
				//������(7-��)start
				cal.add(Calendar.DAY_OF_YEAR, -(7-differ));
				startTime = returnSdf.format(cal.getTime());
			}else{
				//���ڵ��ڵ����
				int differ = wi-param;
				//��ǰ�Ʋ� start
				cal.add(Calendar.DAY_OF_YEAR, -differ);
				startTime = returnSdf.format(cal.getTime());
				//�ָ�
				cal.add(Calendar.DAY_OF_YEAR, +differ);
				//��ǰ��(7-cha-1)end
				cal.add(Calendar.DAY_OF_YEAR, +(7-differ-1));
				endTime = returnSdf.format(cal.getTime());
			}
			
			return startTime+"-"+endTime;
			
		}else if ("month".equals(dataType.toLowerCase())){
			
			if(param > 31 || param < 1){
				//�ڴ��˳�
				return "month_only_1-31";
			}
			
			if(param == 1){
				
				//���� 1 ��������Ϊ��λ
				cal.set(Calendar.DAY_OF_MONTH,1);//���µ�һ��
				startTime = returnSdf.format(cal.getTime());
				
				cal.add(Calendar.MONTH,1);//�������һ��
				cal.set(Calendar.DAY_OF_MONTH,0);
				endTime = returnSdf.format(cal.getTime());
				
				return startTime+"-"+endTime;
				
			}else{
				//�����1�������ǰ� �º�Ϊ��λ
				//��ȡ��ǰ����
				int today = cal.get(Calendar.DAY_OF_MONTH);
				if(today>=param){
					//�����ǰ�������� ���� ����
					//�õ�����µĿ�ʼʱ��������һ����
					cal.add(Calendar.DAY_OF_YEAR, -(today-param));
					startTime = returnSdf.format(cal.getTime());
					cal.add(Calendar.MONTH, 1);
					//��ȥһ��,������β
					cal.add(Calendar.DAY_OF_YEAR, -1);
					endTime = returnSdf.format(cal.getTime());
					return startTime+"-"+endTime;
				}else{
					//�õ��������Ľ���ʱ����ǰ��һ����
					cal.add(Calendar.DAY_OF_YEAR, (param-today-1));
					endTime = returnSdf.format(cal.getTime());
					cal.add(Calendar.MONTH, -1);
					//��һ��
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
