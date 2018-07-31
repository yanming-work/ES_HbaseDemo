package com.test.web.model.es;

import java.io.Serializable;

public class Author implements Serializable {
    private Long id;  //作者id
    private String name;  //作者姓名
    private String remark;  //作者简介
	public Long getId() {
		return id;
	}
	public void setId(Long id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getRemark() {
		return remark;
	}
	public void setRemark(String remark) {
		this.remark = remark;
	}
   
    
    
}