package com.test;

import java.util.Map;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticsearchTest {
	private Logger logger = LoggerFactory.getLogger(ElasticsearchTest.class);

	@Test
	public void test() {
		try {
			
			Map<String,String> jsonMap=null;
			System.out.println("createIndex:"+ElasticSearchUtil.createIndex("msg2", "tweet2"));
			//String jsonStr="{\"id\":22,\"name\":\"test2\",\"age\":25,\"sex\":\"女\"}";
			//System.out.println("insertJsonStr:"+ElasticSearchUtil.insertJsonStr("msg", "tweet", jsonStr));
			
			/**
			long s =System.currentTimeMillis();
			for(int i=40000;i<50000;i++){
				String jsonStri="{\"id\":"+i+",\"name\":\"test"+i+"\",\"age\":22,\"sex\":\"男\"}";
				ElasticSearchUtil.insertJsonStr("msg", "tweet",i+"", jsonStri);
			}
			
			for(int i=50000;i<60000;i++){
				String jsonStri="{\"id\":"+i+",\"name\":\"test"+i+"\",\"age\":25,\"sex\":\"女\"}";
				ElasticSearchUtil.insertJsonStr("msg", "tweet",i+"", jsonStri);
			}
			long e =System.currentTimeMillis();
			//20个消耗时间(秒)：21
			System.out.println("消耗时间(秒)："+(e-s)/1000);
		**/
			
			/**
			mapjson = new HashMap<String, String>();
			for(int i=3000000;i<4000000;i++){
				mapjson.put(i+"", "{\"id\":"+i+",\"name\":\"test"+i+"\"}");
			}
			 s =System.currentTimeMillis();
			ElasticSearchUtil.insertJsonStrMapList("msg", "tweet", mapjson);
			 e =System.currentTimeMillis();
			 ////20个消耗时间(秒)：2
			System.out.println("消耗时间(秒)："+(e-s)/1000);
			**/
			
			
			//System.out.println("selectById:"+ElasticSearchUtil.selectById("msg", "tweet","1"));
			
			//List<Map<String,String>> jsonStrMapList=ElasticSearchUtil.selectByKeyValueTop10("msg", "tweet", "name", "test2");
			//System.out.println("selectByKeyValue:"+jsonStrMapList.size());
			
			
			//List<Map<String,String>> jsonStrMapList2=ElasticSearchUtil.selectByKeyValuesTop10("msg", "tweet", "name", "test2", "test3", "test4", "test5");
			//System.out.println("selectByKeyValues:"+jsonStrMapList2.size());
			
			
			//jsonMap=ElasticSearchUtil.selectAllTop100("msg", "tweet");
			//System.out.println(jsonMap.toString());
			//jsonMap=ElasticSearchUtil.selectAll("msg", "tweet", 100, 10000);
			//System.out.println(jsonMap.size()+"个数据:"+ jsonMap.toString());
			
			
			//jsonMap=ElasticSearchUtil.selectCommonTermsTop100("msg", "tweet", "name", "test0");
			
			//jsonMap=ElasticSearchUtil.selectByMultiMatchQueryTop100("msg", "tweet", "25", "id","age");
			
			//jsonMap=ElasticSearchUtil.selectQueryTop100("msg", "tweet", "女");	
					
			//jsonMap=ElasticSearchUtil.selectSimpleQueryTop100("msg", "tweet", "女");	
					
					
			//jsonMap=ElasticSearchUtil.selectPrefixQueryTop100("msg", "tweet","name", "te");
					
			//jsonMap=ElasticSearchUtil.selectFuzzyQueryTop100("msg", "tweet","name", "test2");
			
			
			//jsonMap=ElasticSearchUtil.selectWildcardQueryTop100("msg", "tweet","name", "test*");
					
					
			jsonMap=ElasticSearchUtil.selectRangeQueryTop100("msg", "tweet","id", 100, 200);
					
					
			if(jsonMap!=null){
				System.out.println(jsonMap.size()+"个数据:"+ jsonMap.toString());
			}else{
				System.out.println("没有查询到数据");
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
