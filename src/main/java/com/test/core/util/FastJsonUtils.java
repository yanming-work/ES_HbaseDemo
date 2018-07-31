package com.test.core.util;
import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.crypto.dsig.keyinfo.KeyValue;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.fastjson.serializer.JSONLibDataFormatSerializer;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.fastjson.serializer.SimpleDateFormatSerializer;
/**  
 * fastjson工具类  
 * @version:1.0.0  
 */ 
public class FastJsonUtils {

	

    private static final SerializeConfig config_dateObj;  
    private static final SerializeConfig config;  
    private static final SerializeConfig config2;  
    private static final SerializeConfig config_utc_8; 
  //格式化日期

    static {  
    	//dateFormat = "yyyy-MM-dd HH:mm:ss";
    	String dateFormat="yyyy/MM/dd HH:mm:ss";
        config = new SerializeConfig();  
        config.put(java.util.Date.class,  new SimpleDateFormatSerializer(dateFormat)); //日期格式化
        
    	String dateFormat2="yyyy/MM/dd HH:mm:ss";
        config2 = new SerializeConfig();  
        config2.put(java.util.Date.class,  new SimpleDateFormatSerializer(dateFormat2)); //日期格式化
       
        String UTC_08_FORMAT="yyyy-MM-dd'T'HH:mm:ss'+08:00'";
        config_utc_8 = new SerializeConfig();  
        config_utc_8.put(java.util.Date.class,  new SimpleDateFormatSerializer(UTC_08_FORMAT)); //日期格式化
        
        
        
        /*****日期对象***"date":{"date":30,"hours":12,"seconds":42,"month":10,"timezoneOffset":-480,"year":117,"minutes":26,"time":1512016002329,"day":4}********/
        config_dateObj = new SerializeConfig();  
        config_dateObj.put(java.util.Date.class, new JSONLibDataFormatSerializer()); // 使用和json-lib兼容的日期输出格式  
        config_dateObj.put(java.sql.Date.class, new JSONLibDataFormatSerializer()); // 使用和json-lib兼容的日期输出格式  
    }  
  
    private static final SerializerFeature[] features = {SerializerFeature.WriteMapNullValue, // 输出空置字段  
            SerializerFeature.WriteNullListAsEmpty, // list字段如果为null，输出为[]，而不是null  
            SerializerFeature.WriteNullNumberAsZero, // 数值字段如果为null，输出为0，而不是null  
            SerializerFeature.WriteNullBooleanAsFalse, // Boolean字段如果为null，输出为false，而不是null  
            SerializerFeature.WriteNullStringAsEmpty, // 字符类型字段如果为null，输出为""，而不是null  
            SerializerFeature.WriteNullBooleanAsFalse, //Boolean字段如果为null,输出为false,而非null	
            SerializerFeature.PrettyFormat //结果是否格式化,默认为false	
    };  
            /***
             * SerializerFeature属性
				名称	含义	备注
				QuoteFieldNames	输出key时是否使用双引号,默认为true	
				UseSingleQuotes	使用单引号而不是双引号,默认为false	
				WriteMapNullValue	是否输出值为null的字段,默认为false	
				WriteEnumUsingToString	Enum输出name()或者original,默认为false	
				UseISO8601DateFormat	Date使用ISO8601格式输出，默认为false	
				WriteNullListAsEmpty	List字段如果为null,输出为[],而非null	
				WriteNullStringAsEmpty	字符类型字段如果为null,输出为"",而非null	
				WriteNullNumberAsZero	数值字段如果为null,输出为0,而非null	
				WriteNullBooleanAsFalse	Boolean字段如果为null,输出为false,而非null	
				SkipTransientField	如果是true，类中的Get方法对应的Field是transient，序列化时将会被忽略。默认为true	
				SortField	按字段名称排序后输出。默认为false	
				WriteTabAsSpecial	把\t做转义输出，默认为false	不推荐
				PrettyFormat	结果是否格式化,默认为false	
				WriteClassName	序列化时写入类型信息，默认为false。反序列化是需用到	
				DisableCircularReferenceDetect	消除对同一对象循环引用的问题，默认为false	
				WriteSlashAsSpecial	对斜杠’/’进行转义	
				BrowserCompatible	将中文都会序列化为\\uXXXX格式，字节数会多一些，但是能兼容IE 6，默认为false	
				WriteDateUseDateFormat	全局修改日期格式,默认为false。JSON.DEFFAULT_DATE_FORMAT = “yyyy-MM-dd”;JSON.toJSONString(obj, SerializerFeature.WriteDateUseDateFormat);	
				DisableCheckSpecialChar	一个对象的字符串属性中如果有特殊字符如双引号，将会在转成json时带有反斜杠转移符。如果不需要转义，可以使用这个属性。默认为false	
				NotWriteRootClassName	含义	
				BeanToArray	将对象转为array输出	
				WriteNonStringKeyAsString	含义	
				NotWriteDefaultValue	含义	
				BrowserSecure	含义	
				IgnoreNonFieldGetter	含义	
				WriteEnumUsingName	含义
             */
 
	
    
    
    public static String toJSONString(Object object) {  
        return JSON.toJSONString(object, config, features);  
    }  
      
   
    
    public static String toJSONNoFeatures(Object object) {  
        return JSON.toJSONString(object, config);  
    } 
    
    
    public static String toJSONString2(Object object) {  
        return JSON.toJSONString(object, config2, features);  
    }  
      
   
    
    public static String toJSONNoFeatures2(Object object) {  
        return JSON.toJSONString(object, config2);  
    }  
      
    
    public static String toJSONStringDateUTC8(Object object) {  
        return JSON.toJSONString(object, config_utc_8, features);  
    }  
      
   
    
    public static String toJSONNoFeaturesDateUTC8(Object object) {  
        return JSON.toJSONString(object, config_utc_8);  
    }  
    
  
  
    public static Object toBean(String text) {  
        return JSON.parse(text);  
    }  
  
    public static <T> T toBean(String text, Class<T> clazz) {  
        return JSON.parseObject(text, clazz);  
    }  
    
    
 // 转换为数组  
    public static <T> Object[] toArray(String text) {  
        return toArray(text, null);  
    }  
  
    // 转换为数组  
    public static <T> Object[] toArray(String text, Class<T> clazz) {  
        return JSON.parseArray(text, clazz).toArray();  
    }  
  
    // 转换为List  
	public static <T> List<T> toList(String text, Class<T> clazz) { 
    	if(text!=null && !"".equals(text)){
    	
    		
    		if(DateUtil.isMaybeHaveDateTime2(text)){
    			 String rexp="\\d{4}\\/\\d{1,2}\\/\\d{1,2}\\s+\\d{1,2}\\:\\d{1,2}\\:\\d{1,2}";
    			 Pattern p = Pattern.compile(rexp);
    			    Matcher m = p.matcher(text);
    			    while(m.find()){
    			    	System.out.println("需要替换的时间字符串："+m.group(0));
    			    	text=text.replace(m.group(0), DateUtil.FormatDate(m.group(0)));
    			    }
    			
    		}
    		 return JSON.parseArray(text, clazz); 
           
    	}else{
    		return null;
    	}
    	 
    }  
  
    /**  
     * 将javabean转化为序列化的json字符串  
     * @param keyvalue  
     * @return  
     */  
    public static Object beanToJson(KeyValue keyvalue) {  
        String textJson = JSON.toJSONString(keyvalue);  
        Object objectJson  = JSON.parse(textJson);  
        return objectJson;  
    }  
      
    /**  
     * 将string转化为序列化的json字符串  
     * @param keyvalue  
     * @return  
     */  
    public static Object textToJson(String text) {  
        Object objectJson  = JSON.parse(text);  
        return objectJson;  
    }  
      
    /**  
     * json字符串转化为map  
     * @param s  
     * @return  
     */  
    @SuppressWarnings("rawtypes")
	public static Map stringToCollect(String s) {  
        Map m = JSONObject.parseObject(s);  
        return m;  
    }  
      
    /**  
     * 将map转化为string  
     * @param m  
     * @return  
     */  
    @SuppressWarnings("rawtypes")
	public static String collectToString(Map m) {  
        String s = JSONObject.toJSONString(m);  
        return s;  
    }  
      
	
	//map<-->JSON
	/**
	 * map-->JSONStr 
	 * MAP转JSON字符串
	 * @param map
	 * @return
	 */
		public static String mapToJsonStr(HashMap<?, ?> map){
			 String jsonStr =null;
			if(map!=null){
				 jsonStr = JSON.toJSONString(map, config, features);
			}
			 //System.out.println("map-->JSONStr");
			 //System.out.println(jsonStr); 
			 return jsonStr;
		}
		
		
		/**
		 * map-->JSONStr 
		 * MAP转JSON字符串
		 * @param map
		 * @return
		 */
		public static String mapToJsonStrNoFeatures(HashMap<?, ?> map){
			 String jsonStr =null;
			if(map!=null){
				 jsonStr = JSON.toJSONString(map);
			}
			 //System.out.println("map-->JSONStr");
			 //System.out.println(jsonStr); 
			 return jsonStr;
		}
		
	
		
		
		/**
		 * map-->JSONObject 
		 * MAP转JSONObject
		 * @param map
		 * @return
		 */
		public static JSONObject mapToJSONObject(HashMap<?, ?> map){
			//将map转换成jsonObject 
			JSONObject jsonObj=null;
			if(map!=null){
				jsonObj= JSONObject.parseObject(JSON.toJSONString(map, config, features));
				//将Map类型的itemInfo转换成json，再经JSONObject转换实现。
			}
			 return jsonObj;
		}
		
		

		/**
		 * map-->JSONObject 
		 * MAP转JSONObject
		 * @param map
		 * @return
		 */
		public static JSONObject mapToJSONObjectNoFeatures(HashMap<?, ?> map){
			//将map转换成jsonObject 
			JSONObject jsonObj=null;
			if(map!=null){
				jsonObj= JSONObject.parseObject(JSON.toJSONString(map));
				//将Map类型的itemInfo转换成json，再经JSONObject转换实现。
			}
			 return jsonObj;
		}
		
		
		
	
		
		
		/**
		 * JSONStr--> map
		 * JSONStr转MAP字符串 
		 * @param jsonStr
		 * @return
		 */
		public static Map<?,?> jsonStrToMap(String jsonStr){
			Map<?,?> mapFromJsonstr=null;
			 if(jsonStr!=null && !"".equals(jsonStr)){
				 mapFromJsonstr= (Map<?,?>)JSON.parse(jsonStr);
				 /**
					System.out.println("JSONStr--> map");
					for (Object key : mapFromJsonstr.keySet()) { 
				         System.out.println(key+":"+mapFromJsonstr.get(key)); 
					}
				**/
			 }
			
			 return mapFromJsonstr;
		}	
		

		/**
		 * JSONObject--> map
		 * JSONObject转MAP字符串 
		 * @param jsonStr
		 * @return
		 */
		@SuppressWarnings("unchecked")
		public static Map<?,?> jsonObjToMap(JSONObject jsonObj){
			Map<String, Object> mapFromJsonObj =null;
			if(jsonObj!=null){
				//将jsonObj转换成Map
				mapFromJsonObj = JSONObject.toJavaObject(jsonObj, Map.class);
				//JOSN.parseObjet()方法同样可以转换
				/**
				System.out.println("JSONObject--> map");
				for (Object key : mapFromJsonObj.keySet()) { 
			         System.out.println(key+":"+mapFromJsonObj.get(key)); 
				}
				**/
			}
			
			
			 return mapFromJsonObj;
		}	
		
		
		
		
		

		//Object<-->JSON
		
		/**
		 * <T> -->JSON
		 * 对象转JSON字符串
		 * @param tobj
		 * @return
		 */
		public static <T> String tobjectToJsonStr(T tobj){
			 String jsonStr =null;
			if(tobj!=null){
				 jsonStr = JSON.toJSONString(tobj, config, features);
				// System.out.println("<T> -->JSON");
				 //System.out.println(jsonStr);
			}
			 
			return jsonStr;
		}
		
		
		/**
		 * <T> -->JSON
		 * 对象转JSON字符串
		 * @param tobj
		 * @return
		 */
		public static <T> String tobjectToJsonStrNoFeatures(T tobj){
			 String jsonStr =null;
			if(tobj!=null){
				 jsonStr = JSON.toJSONString(tobj);
				// System.out.println("<T> -->JSON");
				 //System.out.println(jsonStr);
			}
			 
			return jsonStr;
		}
		
		
		
		
		/**
		 * Object-->JSON
		 * 对象转JSON字符串
		 * @param obj
		 * @return
		 */
		public static  String objectToJsonStr(Object obj){
			 String jsonStr =null;
				if(obj!=null){
					 jsonStr = JSON.toJSONString(obj, config, features);
					// System.out.println("Object -->JSON");
					// System.out.println(jsonStr);
				}
			return jsonStr;
		}
		
		

		/**
		 * Object-->JSON
		 * 对象转JSON字符串
		 * @param obj
		 * @return
		 */
		public static  String objectToJsonStrNoFeatures(Object obj){
			 String jsonStr =null;
				if(obj!=null){
					 jsonStr = JSON.toJSONString(obj);
					// System.out.println("Object -->JSON");
					 //System.out.println(jsonStr);
				}
			return jsonStr;
		}
		
		
		

		
		/**
		 * JSONStr --> Object
		 * @param jsonStr
		 * @param clazz
		 * @return
		 */
		@SuppressWarnings({ "rawtypes", "unchecked" })
		public static Object jsonStrToObject(String jsonStr,Class clazz){
			 Object obj =null;
			 if(jsonStr!=null && !"".equals(jsonStr)){
				 obj= JSON.parseObject(jsonStr, clazz); 
			 }
			 return obj;
		}
		/**
		 * JSONStr --> <T>
		 * @param <T>
		 * @param jsonStr
		 * @param clazz
		 * @return 
		 * @return
		 */
		public static  <T> T jsonStrToObjectT(String jsonStr,Class<T> clazz){
			 T tobj =null;
			 if(jsonStr!=null && !"".equals(jsonStr)){
				 tobj = JSON.parseObject(jsonStr, clazz); 
			 }
			 
			 return tobj;
		}
		
		

		//List<VO><-->JSON
		/**
		 * List<VO>-->JSONStr
		 * @param list
		 * @return
		 */
		public static String listToJsonStr(List<?> list){
			
			  String jsonArrStr =null;
			  if(list!=null && list.size()>0){
				  jsonArrStr = JSON.toJSONString(list, config, features);  
			  }
			return jsonArrStr;
			
		}
		
		/**
		 * List<VO>-->JSONStr
		 * @param list
		 * @return
		 */
		public static String listToJsonStrNoFeatures(List<?> list){
			
			  String jsonArrStr =null;
			  if(list!=null && list.size()>0){
				  jsonArrStr = JSON.toJSONString(list);  
			  }
			return jsonArrStr;
			
		}
		
		
		

		
		
		/**
		 * List<VO>-->JSONArray
		 * @param list
		 * @return
		 */
		public static JSONArray listToJSONArray(List<?> list){
			
			JSONArray jsonArr  =null;
			  if(list!=null && list.size()>0){
				//将List转换成JSONArray
					 jsonArr = JSONArray.parseArray(JSON.toJSONString(list, config, features));
					
			  }
			return jsonArr;
			
		}
		
		
		
		/**
		 * List<VO>-->JSONArray
		 * @param list
		 * @return
		 */
		public static JSONArray listToJSONArrayNoFeatures(List<?> list){
			
			JSONArray jsonArr  =null;
			  if(list!=null && list.size()>0){
				//将List转换成JSONArray
					 jsonArr = JSONArray.parseArray(JSON.toJSONString(list));
					
			  }
			return jsonArr;
			
		}
		
		
		
		/**
		 * JSONArrStr-->List<VO>
		 * @param jsonArrStr
		 * @param clazz
		 * @return
		 */
		public static <T> List<T> jsonArrStrToList(String jsonArrStr,Class<T> clazz ){
			List<T> list=null;
			if(jsonArrStr!=null &&!"".equals(jsonArrStr)){
				list= JSON.parseArray(jsonArrStr,clazz); 
			}
			return list;
			
		}
		
		/***
		public static <T> List<T> jsonArrToList(JSONArray jsonArr,Class<T> clazz ){
			List<T> list=null;
			if(jsonArr!=null &&jsonArr.size()>0){
				list= JSON.parseArray(jsonArr,clazz); 
			}
			return list;
			
		}
		
		
		**/
		
		/**
		 * JSONArrStr-->List<Map>
		 * @param jsonArrStr
		 * @return
		 */
		public static List<Map<String, Object>> jsonArrStrToListMap(String jsonArrStr){
			List<Map<String, Object>> listMap = null;
			if(jsonArrStr!=null &&!"".equals(jsonArrStr)){
				listMap = JSON.parseObject(jsonArrStr, new TypeReference<List<Map<String,Object>>>(){});
			}
			return listMap;
			
		}
		
		/**
		 * JSONArrStr-->List<Map>
		 * @param jsonArrStr
		 * @return
		 */
		public static List<Map<?, ?>> jsonArrStrToListMapT(String jsonArrStr){
			List<Map<?, ?>> listMap = null;
			if(jsonArrStr!=null &&!"".equals(jsonArrStr)){
				listMap = JSON.parseObject(jsonArrStr, new TypeReference<List<Map<?,?>>>(){});
			}
			return listMap;
			
		}
		
		
		
		

	public static void main(String[] args) {
		/**
		 	//map<-->JSON
		  	HashMap<Object, Object> map = new HashMap<>();
		    map.put("key1", "value1");
		    map.put("key2", "value2");
		    mapToJsonStr(map);
		    
		    String jsonStr="{\"key1\":\"value1\",\"key2\":\"value2\"}" ;
		    jsonStrToMap(jsonStr);
		    jsonObjToMap(mapToJSONObject(map));
		**/
		
		/**
			//Object<-->JSON
			UserInfoTest userInfo = new UserInfoTest();
		    userInfo.setAge(30);
		    userInfo.setName("name1");
			tobjectToJsonStr(userInfo);
			objectToJsonStr(userInfo);
			
			
			String userJsonStr="{\"id\":\"\",\"age\":30,\"username\":\"name1\"}";
			UserInfoTest u1=(UserInfoTest) jsonStrToObject(userJsonStr, UserInfoTest.class);
			System.out.println(u1.getAge());
			
			UserInfoTest u2=(UserInfoTest) jsonStrToObjectT(userJsonStr, UserInfoTest.class);
			System.out.println(u2.getAge());
		**/
		
		/**
		
			UserInfoTest ut1 = new UserInfoTest();
		    ut1.setAge(30);
		    ut1.setName("name1");
		    UserInfoTest ut2 = new UserInfoTest();
		    ut2.setAge(20);
		    ut2.setName("name2");
		    List<UserInfoTest> userInfoList = new ArrayList<UserInfoTest>();
		    userInfoList.add(ut1);
		    userInfoList.add(ut2);
		    System.out.println(listToJsonStr(userInfoList));
		    System.out.println(listToJsonStrNoFeatures(userInfoList));
		    JSONArray jsonArr= listToJSONArray(userInfoList);
		    System.out.println(jsonArr.get(0));
	**/
		
		
			UserInfoTest userInfo = new UserInfoTest();
		    userInfo.setAge(30);
		    userInfo.setName("name1");
		    userInfo.setDate(new Date());
		    System.out.println(toJSONString(userInfo));
		    
		    System.out.println(toJSONNoFeatures(userInfo));
	}
	
	
	 public static class UserInfoTest{
		BigDecimal id;
		int age;
		String name;
		Integer count;
		String address;
		Date date;
		
		public BigDecimal getId() {
			return id;
		}
		public void setId(BigDecimal id) {
			this.id = id;
		}
		public int getAge() {
			return age;
		}
		public void setAge(int age) {
			this.age = age;
		}
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public Integer getCount() {
			return count;
		}
		public void setCount(Integer count) {
			this.count = count;
		}
		public String getAddress() {
			return address;
		}
		public void setAddress(String address) {
			this.address = address;
		}
		public Date getDate() {
			return date;
		}
		public void setDate(Date date) {
			this.date = date;
		}
		
		
		
	}
	
}
