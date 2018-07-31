package com.test.core;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import org.springframework.beans.ConversionNotSupportedException;
import org.springframework.beans.TypeMismatchException;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;
import org.springframework.web.HttpMediaTypeNotAcceptableException;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;

import com.test.core.result.ApiReturnObj;

public class ApiResponseAdviceException  {
	

	@ExceptionHandler(value = RuntimeException.class)
	@ResponseBody
	public ApiReturnObj<Object>  handle(RuntimeException ex) {
		// *记入异常日志
		System.out.println("进入异常日志");
		System.out.println("异常:"+ex.toString());
		//输出详细异常
		ex.printStackTrace();
		
		ApiReturnObj<Object> apiReturnObj = new ApiReturnObj<Object> ();
		//apiReturnObj.setDatas(ex.getMessage());
		//ApiReturnObj.setException("\u670d\u52a1\u5f02\u5e38\uff0c\u8bf7\u8054\u7cfb\u7ba1\u7406\u5458\uff01");
		if (ex instanceof NumberFormatException) {// 判断是不是数据转换异常
			apiReturnObj.setCode(-1);
			apiReturnObj.setMsg("NumberFormatException");
		}else if (ex instanceof NullPointerException) {// 判断是不是空指针异常
			apiReturnObj.setCode(-1);
			apiReturnObj.setMsg("NullPointerException");
		}else{
			apiReturnObj.setCode(-1);
			//\u670D\u52A1\u5F02\u5E38
			apiReturnObj.setMsg("服务异常");
		}
	 
      
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream();  
		ex.printStackTrace(new PrintStream(baos));  
		String exception = baos.toString();  
		  // 记录error级别的信息  
        
		return apiReturnObj;
	}

	
	
	//空指针异常
    @ExceptionHandler(NullPointerException.class)  
    @ResponseBody  
    public ApiReturnObj<Object>  nullPointerExceptionHandler(NullPointerException ex) {  
        ex.printStackTrace();
        ApiReturnObj<Object>  apiReturnObj = new ApiReturnObj<Object> ();
		apiReturnObj.setCode(-1);
		apiReturnObj.setMsg("空指针异常");
        return apiReturnObj;
    }   
    //类型转换异常
    @ExceptionHandler(ClassCastException.class)  
    @ResponseBody  
    public ApiReturnObj<Object>  classCastExceptionHandler(ClassCastException ex) {  
        ex.printStackTrace();
        ApiReturnObj<Object>  apiReturnObj = new ApiReturnObj<Object> ();
      		apiReturnObj.setCode(-1);
      		apiReturnObj.setMsg("类型转换异常");
            return apiReturnObj; 
    }  

    //IO异常
    @ExceptionHandler(IOException.class)  
    @ResponseBody  
    public ApiReturnObj<Object>  iOExceptionHandler(IOException ex) {  
    	ApiReturnObj<Object>  apiReturnObj = new ApiReturnObj<Object> ();
    	apiReturnObj.setCode(-1);
    	apiReturnObj.setMsg("IO异常");
         return apiReturnObj; 
    }  
    //未知方法异常
    @ExceptionHandler(NoSuchMethodException.class)  
    @ResponseBody  
    public ApiReturnObj<Object>  noSuchMethodExceptionHandler(NoSuchMethodException ex) {  
        ex.printStackTrace();
        ApiReturnObj<Object>  apiReturnObj = new ApiReturnObj<Object> ();
        apiReturnObj.setCode(-1);
        apiReturnObj.setMsg("未知方法异常");
         return apiReturnObj; 
    }  

    //数组越界异常
    @ExceptionHandler(IndexOutOfBoundsException.class)  
    @ResponseBody  
    public ApiReturnObj<Object>  indexOutOfBoundsExceptionHandler(IndexOutOfBoundsException ex) {  
        ex.printStackTrace();
        ApiReturnObj<Object>  apiReturnObj = new ApiReturnObj<Object> ();
        apiReturnObj.setCode(-1);
        apiReturnObj.setMsg("数组越界异常");
         return apiReturnObj; 
    }
    //400错误
    @ExceptionHandler({HttpMessageNotReadableException.class})
    @ResponseBody
    public ApiReturnObj<Object>  requestNotReadable(HttpMessageNotReadableException ex){
    	 ApiReturnObj<Object>  apiReturnObj = new ApiReturnObj<Object>();
    	 apiReturnObj.setCode(400);
    	 apiReturnObj.setMsg("400错误");
          return apiReturnObj; 
    }
    //400错误
    @ExceptionHandler({TypeMismatchException.class})
    @ResponseBody
    public ApiReturnObj<Object>  requestTypeMismatch(TypeMismatchException ex){
    	 ApiReturnObj<Object>  apiReturnObj = new ApiReturnObj<Object> ();
    	 apiReturnObj.setCode(400);
    	 apiReturnObj.setMsg("400错误");
       return apiReturnObj; 
    }
    //400错误
    @ExceptionHandler({MissingServletRequestParameterException.class})
    @ResponseBody
    public ApiReturnObj<Object>  requestMissingServletRequest(MissingServletRequestParameterException ex){
    	 ApiReturnObj<Object>  apiReturnObj = new ApiReturnObj<Object> ();
    	 apiReturnObj.setCode(400);
    	 apiReturnObj.setMsg("400错误");
       return apiReturnObj; 
    }
    //405错误
    @ExceptionHandler({HttpRequestMethodNotSupportedException.class})
    @ResponseBody
    public ApiReturnObj<Object>  request405(){
    	 ApiReturnObj<Object>  apiReturnObj = new ApiReturnObj<Object> ();
    	 apiReturnObj.setCode(405);
    	 apiReturnObj.setMsg("405错误");
        return apiReturnObj; 
    }
    //406错误
    @ExceptionHandler({HttpMediaTypeNotAcceptableException.class})
    @ResponseBody
    public ApiReturnObj<Object>  request406(){
    	ApiReturnObj<Object>  apiReturnObj = new ApiReturnObj<Object> ();
    	apiReturnObj.setCode(406);
    	apiReturnObj.setMsg("406错误");
        return apiReturnObj; 
    }
    //500错误
    @ExceptionHandler({ConversionNotSupportedException.class,HttpMessageNotWritableException.class})
    @ResponseBody
    public ApiReturnObj<Object>  server500(RuntimeException runtimeException){
    	ApiReturnObj<Object>  apiReturnObj = new ApiReturnObj<Object> ();
    	apiReturnObj.setCode(500);
    	apiReturnObj.setMsg("500错误");
        return apiReturnObj; 
    }
	
	@ExceptionHandler(value = Exception.class)
	@ResponseBody
	public ApiReturnObj<Object>  handle(Exception ex) {
		// *记入异常日志
		ByteArrayOutputStream baos = new ByteArrayOutputStream();  
		ex.printStackTrace(new PrintStream(baos));  
		String exception = baos.toString();  
		  // 记录error级别的信息  
        
        ApiReturnObj<Object>  apiReturnObj = new ApiReturnObj<Object> ();
        if (ex instanceof org.springframework.web.servlet.NoHandlerFoundException) {
        	apiReturnObj.setCode(-2);
        	//\u670d\u52a1\u5f02\u5e38\uff0c\u8bf7\u8054\u7cfb\u7ba1\u7406\u5458
        	apiReturnObj.setMsg("服务异常，请联系管理员");
        }else{
        	apiReturnObj.setCode(-1);
        	//\u5F02\u5E38
        	apiReturnObj.setMsg("异常");
        }
		
		return apiReturnObj;

	}
}
