package com.guoan.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.guoan.utils.MathUtil;

/**
  * Description:  �Զ��庯��,�����ǻ�ȡָ��ʱ��ε���С��һ�� 
  * @author lyy  
  * @date 2018��7��9��
 */
public class DistCalculate  extends UDF{

	/**
	  * Title: evaluate
	  * Description: 
	  * @param lat1γ��(0-90)
	  * @param lng1����(0-180)
	  * @param lat2
	  * @param lng2
	  */
	public Double evaluate(Double lat1 , Double lng1 , Double lat2 , Double lng2 ){
		
		if(lat1<0 || lat1> 90 || lng1<0 || lng1 >180 || lat2<0 || lat2> 90 || lng2<0 || lng2 >180){
			return -1.0 ;
		}else{
			return MathUtil.GetDistance(lat1, lng1, lat2, lng2);
		}
	}

	
	
	public static void main(String[] args) {
		
		DistCalculate dc = new DistCalculate();
		System.out.println(dc.evaluate(29.490295,106.486654,29.615467,106.581515));
	}
	
}
