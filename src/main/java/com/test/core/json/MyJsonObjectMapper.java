package com.test.core.json;

import java.io.IOException;
import java.text.SimpleDateFormat;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.test.core.json.serializer.StringUnicodeSerializer;


public class MyJsonObjectMapper extends ObjectMapper{
		  /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

		public MyJsonObjectMapper(){
		    super();
		    
		    //设置null值不参与序列化(字段不被显示)
		    //Include.Include.ALWAYS 默认 
			//Include.NON_DEFAULT 属性为默认值不序列化 
		    //Include.NON_EMPTY 属性为 空（“”） 或者为 NULL 都不序列化 
		    //Include.NON_NULL 属性为NULL 不序列化 
	       //this.setSerializationInclusion(Include.NON_NULL);  
	      
		    
		    //将 null 转换 为 “”
		   
		    this.getSerializerProvider().setNullValueSerializer(new JsonSerializer<Object>() {  
		      @Override  
		      public void serialize(Object value, JsonGenerator jg, SerializerProvider sp) throws IOException, JsonProcessingException {  
		          jg.writeString("");  
		      }  
		    });
		  
		    
	        // 禁用空对象转换json校验  
	        this.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);  
	        //忽略未知的字段  
	        this.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);  
	        //驼峰命名法转换为小写加下划线  
	        //this.setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);  
		    //设置时间格式
	        this.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
	      /**中文进行Unicode编码**/
	      //当找不到对应的序列化器时 忽略此字段
	        this.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
	      //使Jackson JSON支持Unicode编码非ASCII字符
	        SimpleModule module = new SimpleModule();
	        module.addSerializer(String.class, new StringUnicodeSerializer());
	        this.registerModule(module);
	     
	
	        	

		  }
		}