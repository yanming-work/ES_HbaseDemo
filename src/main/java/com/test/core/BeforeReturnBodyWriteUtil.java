package com.test.core;


import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.http.converter.json.MappingJacksonValue;

import com.test.core.result.ApiReturnObj;
import com.test.core.result.DatatablesResult;
import com.test.core.result.JqGridPageResult;
import com.test.core.result.PicUploadResult;
import com.test.core.result.RoleZTreeResult;
import com.test.core.result.TreeResult;
import com.test.core.result.TreeViewResult;
import com.test.core.result.ZTreeResult;
import com.test.core.util.FastJsonUtils;

public class BeforeReturnBodyWriteUtil {

	public static String WHAT_SERVICE=null;

	@SuppressWarnings("rawtypes")
	public static Object beforeBodyWrite(Object body) {
		
			if (body != null) {
				
				if(body instanceof  MappingJacksonValue){
					MappingJacksonValue mappingJacksonValue=(MappingJacksonValue) body;
					if(mappingJacksonValue!=null){
						body=mappingJacksonValue.getValue();
					}
					
				}
				//如果是返回客户端模板类不进行过滤
				if (body instanceof ApiReturnObj  ||body instanceof DatatablesResult || body instanceof PicUploadResult || body instanceof TreeResult || body instanceof TreeViewResult
					||	body instanceof JqGridPageResult 
						) {
				
					return body;
				} else {
					if(body instanceof List<?>){
						if(((List) body).size()>0){
							Object listObj=((List) body).get(0);
							 if( listObj instanceof ZTreeResult ||	listObj instanceof RoleZTreeResult) {
								 return body; 
							 }else{
								 	ApiReturnObj<List> apiReturnObj= new ApiReturnObj<List>();
									apiReturnObj.setCode(1);
									apiReturnObj.setMsg("成功");
									apiReturnObj.setDatas((List)body);
									return apiReturnObj; 
							 }
							
						}else{
							
							ApiReturnObj apiReturnObj= new ApiReturnObj();
							apiReturnObj.setCode(0);
							apiReturnObj.setMsg( "没有获取到相关数据");
							return apiReturnObj;
						}
					}else if (body instanceof String || body instanceof Byte || body instanceof Short || body instanceof Integer || body instanceof Long || body instanceof Float || body instanceof Double || body instanceof Boolean  ){
					
						
						
						Map<String,Object> map = new HashMap<String,Object>();
						map.put("result", body);
						ApiReturnObj<Object> apiReturnObj= new ApiReturnObj<Object>();
						apiReturnObj.setCode(1);
						apiReturnObj.setMsg("成功");
						apiReturnObj.setDatas(map);
						
						if(body instanceof String && "SpringBoot".equals(WHAT_SERVICE)){
							return FastJsonUtils.objectToJsonStr(apiReturnObj);
						}else{
							return apiReturnObj;
						}
					}else{
						ApiReturnObj<Object> apiReturnObj= new ApiReturnObj<Object>();
						apiReturnObj.setCode(1);
						apiReturnObj.setMsg("成功");
						apiReturnObj.setDatas(body);
						return apiReturnObj; 
					}
					
				
				}
			} else {
				ApiReturnObj apiReturnObj= new ApiReturnObj();
				apiReturnObj.setCode(0);
				apiReturnObj.setMsg( "没有获取到相关数据");
				return apiReturnObj;
			}
		
		
	}
	
	
	
	@SuppressWarnings("rawtypes")
	public static MappingJacksonValue beforeBodyWriteJsonp(Object body) {
		if (body instanceof MappingJacksonValue) {
			return (MappingJacksonValue) body;
		} else {

			if (body instanceof ApiReturnObj  || body instanceof DatatablesResult || body instanceof PicUploadResult
					|| body instanceof TreeResult || body instanceof TreeViewResult
					||	body instanceof JqGridPageResult  || body instanceof String || body instanceof Byte || body instanceof Short || body instanceof Integer || body instanceof Long || body instanceof Float || body instanceof Double || body instanceof Boolean  
						
					
			) {
				return new MappingJacksonValue(body);

			} else {
				if (body instanceof List<?>) {
					if (((List) body).size() > 0) {
						Object listObj = ((List) body).get(0);
						if (listObj instanceof ZTreeResult || listObj instanceof RoleZTreeResult) {
							return new MappingJacksonValue(body);
						} else {
							ApiReturnObj<List> apiReturnObj = new ApiReturnObj<List>();
							apiReturnObj.setCode(1);
							apiReturnObj.setMsg("成功");
							apiReturnObj.setDatas((List) body);
							return new MappingJacksonValue(apiReturnObj);
						}

					} else {

						ApiReturnObj apiReturnObj = new ApiReturnObj();
						apiReturnObj.setCode(0);
						apiReturnObj.setMsg("没有获取到相关数据");
						return new MappingJacksonValue(apiReturnObj);
					}
				}else {
					ApiReturnObj<Object> apiReturnObj = new ApiReturnObj<Object>();
					apiReturnObj.setCode(1);
					apiReturnObj.setMsg("成功");
					apiReturnObj.setDatas(body);
					return new MappingJacksonValue(apiReturnObj);
				}

			}

		}

	}
	
	
	
	
}

