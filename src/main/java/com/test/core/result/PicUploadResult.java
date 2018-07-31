package com.test.core.result;
import java.io.Serializable;

public class PicUploadResult implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private int error;
	
	private String url;
	
	private String width;
	
	private String height;

	public PicUploadResult() {

	}

	public int getError() {
		return error;
	}

	public void setError(int error) {
		this.error = error;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getWidth() {
		return width;
	}

	public void setWidth(String width) {
		this.width = width;
	}

	public String getHeight() {
		return height;
	}

	public void setHeight(String height) {
		this.height = height;
	}

}
