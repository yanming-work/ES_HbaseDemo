package com.test.web.model.es;

import java.io.Serializable;
import java.util.Date;

public class Article2 implements Serializable {
    private Long id;
    private String title;  //标题
    private String abstracts;  //摘要
    private String content;  //内容
    private Long clickCount;  //点击率
	public Long getId() {
		return id;
	}
	public void setId(Long id) {
		this.id = id;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public String getAbstracts() {
		return abstracts;
	}
	public void setAbstracts(String abstracts) {
		this.abstracts = abstracts;
	}
	public String getContent() {
		return content;
	}
	public void setContent(String content) {
		this.content = content;
	}
	
	public Long getClickCount() {
		return clickCount;
	}
	public void setClickCount(Long clickCount) {
		this.clickCount = clickCount;
	}
	
    
    
}
