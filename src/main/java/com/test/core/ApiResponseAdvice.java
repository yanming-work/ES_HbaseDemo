package com.test.core;
import org.springframework.core.MethodParameter;
import org.springframework.http.MediaType;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyAdvice;

import com.test.core.result.ApiReturnObj;

@SuppressWarnings("rawtypes")
@RestControllerAdvice 
/**
 * 补充：如果全部异常处理返回json，那么可以使用 @RestControllerAdvice 代替 @ControllerAdvice ，这样在方法上就可以不需要添加 @ResponseBody。
 *
 */
public class ApiResponseAdvice extends ApiResponseAdviceException    implements ResponseBodyAdvice {
	
	 /**
     * 判断支持的类型 
     */
	@Override
	public boolean supports(MethodParameter returnType, Class converterType) {
		//注意，这里必须返回true，否则不会执行beforeBodyWrite方法 
		return true;
	}

	 /**
     * 过滤
     * 
     * 此处获取到request 是为了取到在拦截器里面设置的一个对象
     */
	@Override
	public Object beforeBodyWrite(Object body, MethodParameter returnType, MediaType selectedContentType,
			Class selectedConverterType, ServerHttpRequest request, ServerHttpResponse response) {
			
		
		//String requestPath = request.getURI().getPath();
        // 将返回的body放到请求上下文中 
        // 普通response不进行过滤
        //if (!requestPath.startsWith("/credit") ){
	
       // }

		
		
		/***返回类型的判断,json的才处理***/
		/**
		if (selectedContentType.includes(MediaType.APPLICATION_JSON)
				|| selectedContentType.includes(MediaType.APPLICATION_JSON_UTF8)
			) {	
			body=BeforeReturnBodyWriteUtil.beforeBodyWrite(body);
		}
		**/
		//void 无返回数据
		String returnTypeName=null;
		try{
			returnTypeName=returnType.getGenericParameterType().getTypeName();
		}catch (Exception e) {
		}
		
		if("void".equals(returnTypeName)){
			return new ApiReturnObj<>(0, "void接口无返回值");
		}else{
			//全部处理
			return BeforeReturnBodyWriteUtil.beforeBodyWrite(body);
		}
		
	}
	
	

	
}