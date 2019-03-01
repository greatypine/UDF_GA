package com.guoan.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;

public class UDFDemo  extends UDF{

	public String evaluate(String str) throws Exception{
		
		if("n".equals(str)){
			throw new UDFArgumentException("erro : ∫Ø ˝ ‰»Î”–ŒÛ");
		}
		
		return str+"aaaaaaaaa111";
	};
}
