package com.test.core.aop;

import java.util.Arrays;
import java.util.Enumeration;

import javax.servlet.http.HttpServletRequest;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.AfterThrowing;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;


/**
 * 实现Web层的日志切面
 */
@Aspect
@Configuration
public class WebLogAspect {
	


	
	ThreadLocal<Long> startTime = new ThreadLocal<Long>();

	/**
	 * 
	 * 定义一个切入点. 解释下：
	 *
	 * ~第一个 *代表任意修饰符及任意返回值. ~第二个 *任意包名 ~第三个 *代表任意方法. ~第四个 *定义在web包或者子包 ~第五个
	 * *任意方法 ~ ..匹配任意数量的参数.
	 */
	// 切入点要拦截的类
	@Pointcut("execution(public * com.test.web.controller..*.*(..))")
	public void webLog() {
	}// 声明一个切入点,切入点的名称其实是一个方法

	
	// 前置通知（不需要获取输入参数）
	@Before("webLog()") // 第一个参数为切入点的名称
	public void doBefore(JoinPoint joinPoint) {
		System.out.println("前置通知");
		startTime.set(System.currentTimeMillis());
		// 接收到请求，记录请求内容
		ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
		HttpServletRequest request = attributes.getRequest();
		
		// 记录下请求内容
		System.out.println("URL : " + request.getRequestURL().toString());
		System.out.println("HTTP_METHOD : " + request.getMethod());
		System.out.println("IP : " + request.getRemoteAddr());
		System.out.println("CLASS_METHOD : " + joinPoint.getSignature().getDeclaringTypeName() + "."
				+ joinPoint.getSignature().getName());
		System.out.println("ARGS : " + Arrays.toString(joinPoint.getArgs()));

		// 获取所有参数方法一：

		Enumeration<String> enu = request.getParameterNames();
		while (enu.hasMoreElements()) {
			String paraName = (String) enu.nextElement();
			System.out.println(paraName + ": " + request.getParameter(paraName));
		}

	}
	
	/**
	//前置通知(获取输入参数)
	@Before("webLog() && args(name)")//第一个参数为切入点的名称,第二个是测试获取输入参数，此处为string类型的，参数名称与方法中的名称相同,如果不获取输入参数，可以不要
	public void doAccessCheck(String name){
	        System.out.println("前置通知(获取输入参数):"+name);
	 }
	**/

	//后置通知（获取返回值）
	// @AfterReturning("webLog()")
	@AfterReturning(pointcut = "webLog()", returning = "result")
	public void doAfterReturning(JoinPoint joinPoint, Object result) {
		
		
		
		System.out.println("后置通知:AfterReturning增强：获取目标方法的返回值：" + result);
		// 接收到请求，记录请求内容
				ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
				HttpServletRequest request = attributes.getRequest();
				
				// 记录下请求内容
				System.out.println("URL : " + request.getRequestURL().toString());
				System.out.println("HTTP_METHOD : " + request.getMethod());
				System.out.println("IP : " + request.getRemoteAddr());
				System.out.println("CLASS_METHOD : " + joinPoint.getSignature().getDeclaringTypeName() + "."
						+ joinPoint.getSignature().getName());
				System.out.println("ARGS : " + Arrays.toString(joinPoint.getArgs()));

				// 获取所有参数方法一：

				Enumeration<String> enu = request.getParameterNames();
				while (enu.hasMoreElements()) {
					String paraName = (String) enu.nextElement();
					System.out.println(paraName + ": " + request.getParameter(paraName));
				}

		
		// 处理完请求，返回内容
		System.out.println("WebLogAspect.doAfterReturning()");
		System.out.println("耗时（毫秒） : " + (System.currentTimeMillis() - startTime.get()));
		
		//日志写数据库
		System.out.println("这里加上日志写入数据库");

	}

	
		//例外通知(获取异常信息)
	     @AfterThrowing(pointcut="webLog()",throwing="e")
	      public void doAfterThrowing(JoinPoint joinPoint,Exception e){
	         System.out.println("例外通知(异常):"+e);
	     }
	     
	     //最终通知
	     @After("webLog()")
	     public void doAfter(){
	         System.out.println("最终通知");
	     }
	     
	     //环绕通知（特别适合做权限系统）
	     @Around("webLog()")
	     public Object doAround(ProceedingJoinPoint pjp) throws Throwable{
	         System.out.println("环绕通知进入方法");
	         Object object=pjp.proceed();
	         System.out.println("环绕通知退出方法");
	         return object;
	     }

}
