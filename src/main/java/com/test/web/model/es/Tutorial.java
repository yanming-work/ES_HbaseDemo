package com.test.web.model.es;

import java.io.Serializable;

public class Tutorial implements Serializable{
    private Long id; //教程id
    private String name;//教程名称
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
    
    
}