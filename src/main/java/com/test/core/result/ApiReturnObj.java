package com.test.core.result;



import java.io.Serializable;

import com.test.core.util.PropertiesUtil;



public class ApiReturnObj<T> implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private int code;
	private String msg;
	private T datas;
	
	
	public ApiReturnObj() {
		
	}
	
	public ApiReturnObj(int code,T datas) {
		super();
		this.code = code;
		String message=PropertiesUtil.getValueByKey("ErrorCode.properties",String.valueOf(code));
		if(message!=null && !"".equals(message)){
			this.msg = message;
		}
		this.datas = datas;
	}
	
	
	public ApiReturnObj(int code, String msg, T datas) {
		super();
		this.code = code;
		this.msg = msg;
		this.datas = datas;
	}
	
	
	public ApiReturnObj(int code, String msg) {
		super();
		this.code = code;
		this.msg = msg;
		
	}
	
	
	
	public ApiReturnObj(int code ) { 
		super();
		this.code = code;
		String message=PropertiesUtil.getValueByKey("ErrorCode.properties",String.valueOf(code));
		if(message!=null && !"".equals(message)){
			this.msg = message;
		}
	}
	
	
	public int getCode() {
		return code;
	}
	public void setCode(int code) {
		this.code = code;
	}
	public String getMsg() {
		return msg;
	}
	public void setMsg(String msg) {
		this.msg = msg;
	}

	public Object getDatas() {
		return datas;
	}

	public void setDatas(T datas) {
		this.datas = datas;
	}


}
