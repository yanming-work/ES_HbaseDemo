package com.test.core.filter;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;

import com.test.core.BeforeReturnBodyWriteUtil;  
  
/** 
 *  
 *  跨域过滤器 
 */  
public class MyCorsFilter implements Filter {  
  
    final static org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MyCorsFilter.class);  
  
  
    public void doFilter(ServletRequest req, ServletResponse res, FilterChain chain) throws IOException, ServletException {  
        HttpServletResponse response = (HttpServletResponse) res;  
        response.setHeader("Access-Control-Allow-Origin", "*");  
        response.setHeader("Access-Control-Allow-Methods", "POST, GET, OPTIONS, DELETE");  
        response.setHeader("Access-Control-Max-Age", "3600");  
        response.setHeader("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");  
        chain.doFilter(req, res);  
        
        System.out.println("*********************************Cors过滤器被使用**************************");  
    }  
    
    
    public void init(FilterConfig filterConfig) throws ServletException {  
    	//获取 init-param 配置的变量
    	String whatService=filterConfig.getInitParameter("whatService");
    	BeforeReturnBodyWriteUtil.WHAT_SERVICE=whatService;
          
    }  
    
    public void destroy() {}  
}  