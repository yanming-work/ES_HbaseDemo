package com.test;

public class EsIndexSource {

	private String name;
	private String type;
	private boolean index;
	private boolean store;
	private boolean fielddata;
	
	
	
	
	
	
	
	public EsIndexSource() {
		super();
	}
	public EsIndexSource(String name, String type, boolean index, boolean store) {
		super();
		this.name = name;
		this.type = type;
		this.index = index;
		this.store = store;
	}
	
	
	public EsIndexSource(String name, String type, boolean index, boolean store, boolean fielddata) {
		super();
		this.name = name;
		this.type = type;
		this.index = index;
		this.fielddata = fielddata;
		this.store = store;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public boolean isIndex() {
		return index;
	}
	public void setIndex(boolean index) {
		this.index = index;
	}
	public boolean isFielddata() {
		return fielddata;
	}
	public void setFielddata(boolean fielddata) {
		this.fielddata = fielddata;
	}
	public boolean isStore() {
		return store;
	}
	public void setStore(boolean store) {
		this.store = store;
	}
	
	
	
	

}
