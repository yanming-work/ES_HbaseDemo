package com.test.core.result;

import java.lang.reflect.Field;
import java.util.LinkedHashMap;

	  
	
	/**
	 * 
	* @ClassName: ClassFieldHelper 
	* @Description: 获取属性与对应的中文名工具类 
	* @author YanMing
	* @date 2017年3月29日 上午11:54:26 
	*
	 */
	public class ClassFieldHelper {  
	   /** 
	    * 根据实体类名获取字段名称和中文名称 
	    * @param entityName 实体类名 
	    * @return List<Map<String,Object>>  
	    */  
	    public static LinkedHashMap<String,LinkedHashMap<String,Object>>   initAnnoFieldDic(@SuppressWarnings("rawtypes") Class clzz){  
	            //用于存储字段和中文值的集合  
	            LinkedHashMap<String,LinkedHashMap<String,Object>>  fieldMap =new LinkedHashMap<>();  
	            //用于存储实体类字段(key)和中文名(value)  
	            LinkedHashMap<String,Object> descriptionMap = new LinkedHashMap<>();  
	            LinkedHashMap<String,Object> dbColumnNameMap = new LinkedHashMap<>();  
	           //获取对象中所有的Field  
	            Field[] fields = clzz.getDeclaredFields();  
	            //循环实体类字段集合,获取标注@ColumnConfig的字段  
	            for (Field field : fields) {  
	                if(field.isAnnotationPresent(ColumnConfig.class)){  
	                    //获取字段名  
	                    String fieldNames = clzz.getSimpleName()+"."+field.getName();  
	                     //获取字段注解  
	                     ColumnConfig columnConfig = field.getAnnotation(ColumnConfig.class);  
	                    //判断是否已经获取过该code的字典数据 避免重复获取  
	                     if(columnConfig.description()!=null&&descriptionMap.get(columnConfig.description())==null){  
	                    	 descriptionMap.put(fieldNames, columnConfig.description());  
	                     } 
	                     
	                     if(columnConfig.dbColumnName()!=null&&dbColumnNameMap.get(columnConfig.dbColumnName())==null){  
	                    	 dbColumnNameMap.put(fieldNames, columnConfig.dbColumnName());  
	                     }  
	            }  
	            }  
	            fieldMap.put("description", descriptionMap);
	            fieldMap.put("dbColumnNameMap", dbColumnNameMap);
	            return fieldMap;  
	    }  
	  
	      
	}  