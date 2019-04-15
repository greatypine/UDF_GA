package com.guoan.udf;

import java.math.BigDecimal;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
  * Description: ë������udf
  * @author lyy  
  * @date 2018��12��18��
 */
public class Profit_item extends UDF{
	
	public Double evaluate(
			double trading_price , int quantity,
			String joint_ims,String order_tag3, int teptag1,String business_type,
			double cost_price , double c_cost_price, 
			String contract_method, double  contract_percent , double contract_price ,
			double proration_seller_price, String department_id)  {
		
		double profit = 0.0;
		try{
			//�Ƿ����׶���[������ҵȺΪ(��������)]
			if(joint_ims != null && "yes".equals(joint_ims) && "8ac28b935fed0bc8015fed4c76f60018".equals(department_id)){
				//����
				//���׶�������һ���Ƶ��µ���ϸ��Ʒ�ڵ��������۱��У���һ��δ�ҵ�����ɱ������㣬���󲻼��㣬������ǩorder_tag3Ϊ0��
				//����֧���������տ������Ϊposting�ģ�����Ϊ0��������ǩorder_tag3Ϊ2;
				if("0".equals(order_tag3) || "2".equals(order_tag3) ){
					return 0.0;
				}
				//���������Ӽ�
				profit =  normal_calculate(trading_price , c_cost_price, "price", 0, 0, quantity , 1);
				return profit ;
			}else if(joint_ims != null && "no".equals(joint_ims)){
				//������
				//������û�к�ͬ
				if(contract_method == null || "".equals(contract_method) ||  "no".equals(contract_method) ){
					//û��ͬ,�ж��Ƿ�self
					if(business_type != null && "self".equals(business_type) ){
						profit =  normal_calculate(trading_price , cost_price, "price", 0, 0, quantity , teptag1);
						return profit;
					}else{
						return 0.0;
					}
				}else{
					//�к�ͬ
					if ("2".equals(order_tag3)){
						//����֧���������տ������Ϊposting�ģ�����Ϊ0��������ǩorder_tag3Ϊ2;
						return 0.0;
					}else if ("percent".equals(contract_method) && proration_seller_price > 0){
						//����������ռ�Ƚ�����0�ģ�percent and seller_price>0 ��,������=��gmv_price-seller_price��*percent;
						profit = new BigDecimal(trading_price+"").subtract(new BigDecimal(proration_seller_price+"")).
						multiply(new BigDecimal(contract_percent+"")).multiply(new BigDecimal(quantity+"")).doubleValue();
						return profit;
					}else{
						//��������������ë��
						 profit = normal_calculate(trading_price, cost_price, contract_method, contract_price, contract_percent, quantity,teptag1);
						 return profit;
					}
				}
			}else{
				return 0.0;
			}
		}catch(Exception e ){
			return 88888888.0;
		}
	}
	
	/**
	  * Title: normal_calculate
	  * Description:  ������������ͬ����ë�� / ������������ë������
	  * @param contract_method
	  * @param trading_price
	  * @param contract_percent
	  * @param quantity
	  * @param teptag1
	  * @return
	 */
	public double normal_calculate(
			double trading_price , double cost_price, String contract_method,
			double contract_price , double contract_percent , int quantity , int teptag1 ){
		double profit = 0.0;
		//��������
		if("percent".equals(contract_method)){//����
			profit = new BigDecimal(trading_price+"").multiply(new BigDecimal(contract_percent+"")).multiply(new BigDecimal(quantity+"")).doubleValue();
			return profit;
		}else if("price".equals(contract_method)){//�Ӽ�
			if (cost_price == 0 && teptag1 == 0){
				return 0.0;
			}
			profit = new BigDecimal(trading_price+"").subtract(new BigDecimal(cost_price+"")).multiply(new BigDecimal(quantity+"")).doubleValue() ;
			return profit;
		}else if ("volume".equals(contract_method)){//����
			profit= new BigDecimal(quantity+"").multiply(new BigDecimal(contract_price+"")).doubleValue();
			return profit;
		}
		return 0.0;
		
	}
	
	
	
/*	1. double trading_price 
 	2. int quantity
	3. String joint_ims
	4. String order_tag3
	5. int teptag1
	6. String business_type
	7. double cost_price
	8. double c_cost_price 
	9. String contract_method
 	10. double  contract_percent
	11. double contract_price ,
	12. double proration_seller_pric
*/
	
	public static void main(String[] args) {
		Profit_item pi = new Profit_item();
		Double evaluate6 = pi.evaluate(12.5,2,"yes","",0,"self",9.970000000000001,0,"",0,0,0,"");
		
		System.out.println(evaluate6);
		
	}
	
	
	
}
