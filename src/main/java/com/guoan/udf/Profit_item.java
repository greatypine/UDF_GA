package com.guoan.udf;

import java.math.BigDecimal;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
  * Description: 毛利计算udf
  * @author lyy  
  * @date 2018年12月18日
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
			//是否优易订单[新增事业群为(国安优易)]
			if(joint_ims != null && "yes".equals(joint_ims) && "8ac28b935fed0bc8015fed4c76f60018".equals(department_id)){
				//优易
				//优易订单，若一条计单下的明细商品在当天日销售表中，有一个未找到，则成本不计算，利润不计算，订单标签order_tag3为0；
				//过账支付订单，收款表类型为posting的，利润为0，订单标签order_tag3为2;
				if("0".equals(order_tag3) || "2".equals(order_tag3) ){
					return 0.0;
				}
				//正常都按从价
				profit =  normal_calculate(trading_price , c_cost_price, "price", 0, 0, quantity , 1);
				return profit ;
			}else if(joint_ims != null && "no".equals(joint_ims)){
				//非优易
				//看看有没有合同
				if(contract_method == null || "".equals(contract_method) ||  "no".equals(contract_method) ){
					//没合同,判断是否self
					if(business_type != null && "self".equals(business_type) ){
						profit =  normal_calculate(trading_price , cost_price, "price", 0, 0, quantity , teptag1);
						return profit;
					}else{
						return 0.0;
					}
				}else{
					//有合同
					if ("2".equals(order_tag3)){
						//过账支付订单，收款表类型为posting的，利润为0，订单标签order_tag3为2;
						return 0.0;
					}else if ("percent".equals(contract_method) && proration_seller_price > 0){
						//从率且卖家占比金额大于0的（percent and seller_price>0 ）,则利润=（gmv_price-seller_price）*percent;
						profit = new BigDecimal(trading_price+"").subtract(new BigDecimal(proration_seller_price+"")).
						multiply(new BigDecimal(contract_percent+"")).multiply(new BigDecimal(quantity+"")).doubleValue();
						return profit;
					}else{
						//正常第三方计算毛利
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
	  * Description:  正常第三方合同计算毛利 / 或者其他计算毛利方法
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
		//正常计算
		if("percent".equals(contract_method)){//从率
			profit = new BigDecimal(trading_price+"").multiply(new BigDecimal(contract_percent+"")).multiply(new BigDecimal(quantity+"")).doubleValue();
			return profit;
		}else if("price".equals(contract_method)){//从价
			if (cost_price == 0 && teptag1 == 0){
				return 0.0;
			}
			profit = new BigDecimal(trading_price+"").subtract(new BigDecimal(cost_price+"")).multiply(new BigDecimal(quantity+"")).doubleValue() ;
			return profit;
		}else if ("volume".equals(contract_method)){//从量
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
