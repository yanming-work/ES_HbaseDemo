package com.test.core.result;

import java.io.Serializable;
import java.util.List;



public class PageInfo<T>  implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -8193442326640720512L;
	 /**
     * 页码
     */
    private int pageNum;
    /**
     * 每页大小
     */
    private int pageSize;
    /**
     * 开始行
     */
    private int startRow;
    /**
     * 结束行
     */
    private int endRow;
    /**
     * 总记录数
     */
    private long total;
    /**
     * 总页数
     */
    private int pages;
   
    /**
     * 排序字段
     */
    private String sortField;
    /**
     * 排序类型
     */
    private String sortType;
    
    /**
     * 返回数据
     */
    private List<T> data;
    
    
    
    
    
	
	public PageInfo() {
		super();
	}
	public PageInfo(int pageNum, int pageSize, int startRow, int endRow, long total, int pages, String sortField,
			String sortType, List<T> data) {
		super();
		this.pageNum = pageNum;
		this.pageSize = pageSize;
		this.startRow = startRow;
		this.endRow = endRow;
		this.total = total;
		this.pages = pages;
		this.sortField = sortField;
		this.sortType = sortType;
		this.data = data;
		
	}
	
	
	
	public List<T> getData() {
		return data;
	}
	
	public void setData(List<T> data) {
		this.data = data;
	}
	public int getPageNum() {
		return pageNum;
	}
	public void setPageNum(int pageNum) {
		this.pageNum = pageNum;
	}
	public int getPageSize() {
		return pageSize;
	}
	public void setPageSize(int pageSize) {
		this.pageSize = pageSize;
	}
	public int getStartRow() {
		return startRow;
	}
	public void setStartRow(int startRow) {
		this.startRow = startRow;
	}
	public int getEndRow() {
		return endRow;
	}
	public void setEndRow(int endRow) {
		this.endRow = endRow;
	}
	public long getTotal() {
		return total;
	}
	public void setTotal(long total) {
		this.total = total;
	}
	public int getPages() {
		return pages;
	}
	public void setPages(int pages) {
		this.pages = pages;
	}
	public String getSortField() {
		return sortField;
	}
	public void setSortField(String sortField) {
		this.sortField = sortField;
	}
	public String getSortType() {
		return sortType;
	}
	public void setSortType(String sortType) {
		this.sortType = sortType;
	}
    
    
}
