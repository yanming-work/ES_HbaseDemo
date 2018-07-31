package com.test.core.json;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;

import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.AbstractHttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.fastjson.serializer.SimpleDateFormatSerializer;
import com.test.core.BeforeReturnBodyWriteUtil;

public class MappingFastJsonHttpMessageConverter extends AbstractHttpMessageConverter<Object>{  
    public static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");  
         
       // fastjson特性参数  
      // private SerializerFeature[] serializerFeature;  
       
    private static final SerializeConfig config;  
    //格式化日期
      private static String dateFormat;

      static {  
      	dateFormat = "yyyy-MM-dd HH:mm:ss";
          config = new SerializeConfig();  
          config.put(java.util.Date.class,  new SimpleDateFormatSerializer(dateFormat)); //日期格式化
      }  
	
    private   SerializerFeature[] serializerFeature = {SerializerFeature.WriteMapNullValue, // 输出空置字段  
            SerializerFeature.WriteNullListAsEmpty, // list字段如果为null，输出为[]，而不是null  
            SerializerFeature.WriteNullNumberAsZero, // 数值字段如果为null，输出为0，而不是null  
            SerializerFeature.WriteNullBooleanAsFalse, // Boolean字段如果为null，输出为false，而不是null  
            SerializerFeature.WriteNullStringAsEmpty, // 字符类型字段如果为null，输出为""，而不是null  
            SerializerFeature.WriteNullBooleanAsFalse, //Boolean字段如果为null,输出为false,而非null	
            SerializerFeature.PrettyFormat //结果是否格式化,默认为false	
    }; 
    
       
 
       public SerializerFeature[] getSerializerFeature() {  
           return serializerFeature;  
       }  
 
       public void setSerializerFeature(SerializerFeature[] serializerFeature) {  
           this.serializerFeature = serializerFeature;  
       }  
 
       public MappingFastJsonHttpMessageConverter() {  
           super(new MediaType("application", "json", DEFAULT_CHARSET));  
       }  
 
       @Override  
       public boolean canRead(Class<?> clazz, MediaType mediaType) {  
           // JavaType javaType = getJavaType(clazz);  
           // return this.objectMapper.canDeserialize(javaType) &&  
           // canRead(mediaType);  
           return true;  
       }  
 
       @Override  
       public boolean canWrite(Class<?> clazz, MediaType mediaType) {  
           // return this.objectMapper.canSerialize(clazz) && canWrite(mediaType);  
           return true;  
       }  
 
       @Override  
       protected boolean supports(Class<?> clazz) {  
           // should not be called, since we override canRead/Write instead  
           throw new UnsupportedOperationException();  
       }  
 
       @Override  
       protected Object readInternal(Class<?> clazz, HttpInputMessage inputMessage)  
               throws IOException, HttpMessageNotReadableException {  
           ByteArrayOutputStream baos = new ByteArrayOutputStream();  
           int i;  
           while ((i = inputMessage.getBody().read()) != -1) {  
               baos.write(i);  
           }  
           return JSON.parseArray(baos.toString(), clazz);  
       }  
 
       @Override  
       protected void writeInternal(Object obj, HttpOutputMessage outputMessage)  
               throws IOException, HttpMessageNotWritableException {  
          // String jsonString = JSON.toJSONString(o, serializerFeature);  
    	  // String jsonString =  FastJsonUtils.objectToJsonStr(obj);
    	   
    	   Object body=BeforeReturnBodyWriteUtil.beforeBodyWrite(obj);
    	   
    	   String jsonString = JSON.toJSONString(body, config, serializerFeature); 
           OutputStream out = outputMessage.getBody();  
           out.write(jsonString.getBytes(DEFAULT_CHARSET));  
           out.flush();  
       }  
}  