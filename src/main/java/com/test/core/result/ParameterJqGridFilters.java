package com.test.core.result;
import java.util.List;

public class ParameterJqGridFilters implements java.io.Serializable{  
	
	/** 
	* @Fields serialVersionUID : TODO(用一句话描述这个变量表示什么) 
	*/ 
	private static final long serialVersionUID = 1L;
	private String groupOp;		//多字段查询时分组类型，主要是AND或者OR
	private List<ParameterJqGridSearchRules> rules; //多字段查询时候，查询条件的集合
	public String getGroupOp() {
		return groupOp;
	}
	public void setGroupOp(String groupOp) {
		this.groupOp = groupOp;
	}
	public List<ParameterJqGridSearchRules> getRules() {
		return rules;
	}
	public void setRules(List<ParameterJqGridSearchRules> rules) {
		this.rules = rules;
	}
	
	
}
