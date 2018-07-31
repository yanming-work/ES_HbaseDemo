package com.test.web.controller;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.test.core.service.HbaseService;
import com.test.core.util.FastJsonUtils;
import com.test.web.model.es.Article;
import com.test.web.model.es.Article2;

@RestController
@RequestMapping("/hbase")
public class HbaseController {

	
	@RequestMapping("/getListById")
	@ResponseBody
	public String getListById() {
		HbaseService.getListByRowkey("t_user", "001");
		return "成功";
	}
	
	
	
	@RequestMapping("/test")
	@ResponseBody
	public String test() {
        
        /*
         * createTable("t2", new String[] { "cf1", "cf2" });
         * listTables();
         * insterRow("t2", "rw1", "cf1", "q1", "val1"); 
         * getData("t2", "rw1","cf1", "q1"); 
         * scanData("t2", "rw1", "rw2");
         * deleRow("t2","rw1","cf1","q1"); 
         * deleteTable("t2");
         */
		/**
		String tableName="t2";
		String[] colFamilys={"cf1", "cf2" };
		String rowkey="rw1";
		String colFamily="cf1";
		String col="q1";
		String val="val1";
		System.out.println("创建表："+tableName+"2");
		HbaseService.createTable(tableName+"2", "aa");
		
		System.out.println("创建表："+tableName);
		HbaseService.createTable(tableName, colFamilys);
		System.out.println("查看已有表");
		HbaseService.listAllTables();
		System.out.println("插入数据insterRow：");
		HbaseService.insterRow(tableName, rowkey, colFamily, col, val);
		for(int i=0;i<1000;i++){
			HbaseService.insterRow(tableName, rowkey+i, colFamily, col+i, val+i);
		}
		
		
		System.out.println("查询表getData：");
		
		Map<String,Map<String,Object>> familyMap =HbaseService.getData(tableName, rowkey+"0");
		System.out.println("familyMap:"+familyMap);
		
		Map<String,Map<String,Object>> familyMap2 =HbaseService.getData(tableName, rowkey+"0", colFamily, col+"0");
		System.out.println("familyMap2:"+familyMap2);

		ArrayList<Map<String, Object>> familyMap3 =HbaseService.scanData(tableName, colFamily);
		System.out.println("批量查询表scanData："+familyMap3);
		
		System.out.println("查询表getListById：");
		HbaseService.getListByRowkey(tableName, rowkey);
		**/
		//创建表
		String tableName2="article2";
		String[] colFamilys={"baseInfo","otherInfo"};
		String[] cols2={"id","title","abstracts","content","postTime","clickCount"};
		String colFamily1="baseInfo";
		String colFamily2="otherInfo";
		
		HbaseService.deleteTable(tableName2);
		
		System.out.println("创建表："+HbaseService.createTable(tableName2, colFamilys));
		Article2 article = new Article2();
		article.setId(0L);
		article.setTitle("springboot integreate elasticsearch");
		article.setAbstracts("springboot integreate hbase is very easy");
		article.setContent("hbase based on lucene," + "spring-data-elastichsearch based on hbase"
				+ ",this tutorial tell you how to integrete springboot with hbase");
		article.setClickCount(1L);
		HbaseService.insterObject(tableName2, "0", colFamily1, article);
		Map<String, String> colMap = new HashMap<String, String>();
		colMap.put("test", "测试");
		colMap.put("msg", "成功");
		HbaseService.insterBatchRowMap(tableName2, "0", colFamily2, colMap);
		
		ArrayList<Map<String, Map<String, Object>>> articleFamilyMap =HbaseService.scanData(tableName2, colFamily1);
		System.out.println("articleFamilyMap:"+articleFamilyMap);
		
		List<Article> articleList = new ArrayList<Article>();
		
		Article article1 = new Article();
		article1.setId(3L);
		article1.setTitle("springboot integreate elasticsearch");
		article1.setAbstracts("springboot integreate hbase is very easy");
		article1.setContent("hbase based on lucene," + "spring-data-elastichsearch based on hbase"
				+ ",this tutorial tell you how to integrete springboot with hbase");
		article1.setClickCount(1L);
		
		
		Article article2 = new Article();
		article2.setId(4L);
		article2.setTitle("springboot integreate elasticsearch");
		article2.setAbstracts("springboot integreate hbase is very easy");
		article2.setContent("hbase based on lucene," + "spring-data-elastichsearch based on hbase"
				+ ",this tutorial tell you how to integrete springboot with hbase");
		article2.setClickCount(1L);
		
		articleList.add(article1);
		articleList.add(article2);
		HbaseService.insterBatchObjectList(tableName2, colFamily1, articleList);
		
		
	
		
		ArrayList<Map<String, Map<String, Object>>> articleFamilyMap2 =HbaseService.scanData(tableName2);
		System.out.println("articleFamilyMap2:");
		System.out.println(FastJsonUtils.toJSONString(articleFamilyMap2));
		return "成功";
	}
}
