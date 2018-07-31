package com.test.core.result;
public class ParameterJqGridSearchRules implements java.io.Serializable{  
    
  /** 
	* @Fields serialVersionUID : TODO(用一句话描述这个变量表示什么) 
	*/ 
	private static final long serialVersionUID = 1L;
private String field;   //查询字段  
  private String op;      //查询操作  
  private String data;    //选择的查询值 
	public String getField() {
		return field;
	}
	public void setField(String field) {
		this.field = field;
	}
	public String getOp() {
		return op;
	}
	public void setOp(String op) {
		this.op = op;
	}
	public String getData() {
		return data;
	}
	public void setData(String data) {
		this.data = data;
	}

  
  
}
