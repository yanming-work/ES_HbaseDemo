package com.test;


import java.io.InputStream;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;
import java.util.Set;  
 
 
/** 
*  
* PropertiesUtil.java 
* 
* @desc properties 资源文件解析工具 
*/  
public class PropertiesUtil {    
	private static Properties prop = new Properties();

	
	
	/**
	 * 初始化Properties实例
	 * @param propertyName
	 * @throws Exception
	 */
	public synchronized static void initProperty(String propertyName) throws Exception {
			prop = new Properties();
			InputStream inputstream = null;
			ClassLoader cl = PropertiesUtil.class.getClassLoader(); 
			//System.out.println(cl);
			if  (cl !=  null ) {        
				inputstream = cl.getResourceAsStream( propertyName );        
			}  else {        
				inputstream = ClassLoader.getSystemResourceAsStream(propertyName );        
			}   

			if (inputstream == null) {
				throw new Exception("inputstream " + propertyName+ " open null");
			}else{
				if(prop!=null){
					prop.load(inputstream);
					inputstream.close();
					inputstream = null;
				}
				
			}
			
	}
    /**
     * 读取数据
     * @param propertyName
     * @param key
     * @return
     */
	public static String getValueByKey(String propertyName, String key) {
		String result = null;
		try {
			initProperty(propertyName);
			if(prop!=null){
				result = prop.getProperty(key);
				prop=null;//GC回收
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
	
	
	

	
	
	
	
	
	/** 
	 * 输出properties的key和value 
	 */  
	@SuppressWarnings("unused")
	public static void printPropTest() {  
		
		try {
			initProperty("ServicesIpPort.properties");
			if(prop!=null){
				
				 long st=0;
				 long et=0;
				 System.out.println("---------（方式一）------------");  
				 	st=System.currentTimeMillis();
				    for (String key : prop.stringPropertyNames()) {  
				    	String k = key;
				    	 String value =prop.getProperty(key);
				        //System.out.println(k + "=" +value);  
				    }  
				    et=System.currentTimeMillis();
				    System.out.println("（方式一）:"+(et-st)+"毫秒");  
				    System.out.println("---------（方式二）------------");  
				    st=System.currentTimeMillis();
				    Set<Object> keys = prop.keySet();//返回属性key的集合  
				    for (Object key : keys) {  
				    	 String k = key.toString();
				    	 String value =(String) prop.get(key);
				        //System.out.println(k + "=" + value);  
				    }  
				    et=System.currentTimeMillis();
				    System.out.println("（方式二）:"+(et-st)+"毫秒");  
				    System.out.println("---------（方式三）------------"); 
				    st=System.currentTimeMillis();
				    Set<Map.Entry<Object, Object>> entrySet = prop.entrySet();//返回的属性键值对实体  
				    for (Map.Entry<Object, Object> entry : entrySet) {  
				    	 String key = (String) entry.getKey() ;
				    	 String value =(String)  entry.getValue();
				        //System.out.println(key + "=" + value);  
				    }  
				    et=System.currentTimeMillis();
				    System.out.println("（方式三）:"+(et-st)+"毫秒");  
				    System.out.println("---------（方式四）------------");  
				    st=System.currentTimeMillis();
				    Enumeration<?> e = prop.propertyNames();  
				    while (e.hasMoreElements()) {  
				        String key = (String) e.nextElement();  
				        String value = prop.getProperty(key);  
				        //System.out.println(key + "=" + value);  
				    }  
				    et=System.currentTimeMillis();
				    System.out.println("（方式四）:"+(et-st)+"毫秒");  
				
				prop=null;//GC回收
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	   
	}  
	
	
	/**
    * 从系统属性文件中获取相应的值
    *
    * @param key key
    * @return 返回value
    */
   public final static String key(String key) {
       return System.getProperty(key);
   }

  
	public static void main(String[] s) {
		
		try {
			
			printPropTest();

		} catch (Exception e) {
			
			e.printStackTrace();
		}
		
		
	}
}