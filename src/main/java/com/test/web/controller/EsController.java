package com.test.web.controller;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.alibaba.fastjson.JSONObject;
import com.test.core.result.PageInfo;
import com.test.core.service.ElasticsearchService;
import com.test.core.util.DateUtil;
import com.test.core.util.FastJsonUtils;
import com.test.web.model.es.Article;
import com.test.web.model.es.Author;
import com.test.web.model.es.Tutorial;

@RestController
@RequestMapping("/es")
public class EsController {

	/**
	 * 测试索引
	 */
	private String indexName = "test_index";// 只允许小写的

	/**
	 * 类型
	 */
	private String esType = "user";

	/**
	 * 创建索引
	 * 
	 * @param request
	 * @param response
	 * @return
	 */
	@RequestMapping("/createIndex")
	@ResponseBody
	public String createIndex() {
		if (!ElasticsearchService.isIndexExist(indexName)) {
			ElasticsearchService.createIndex(indexName);
		} else {
			return "索引已经存在";
		}
		return "索引创建成功";
	}
	
	
	@RequestMapping("/deleteIndex")
	@ResponseBody
	public boolean deleteIndex() {
		return ElasticsearchService.deleteIndex(indexName);
	}

	@RequestMapping("/allIndex")
	@ResponseBody
	public Set<String> getAllIndices() {
		return ElasticsearchService.getAllIndices();
	}


	@RequestMapping("/getAllType")
	@ResponseBody
	public String[] getMapping() {
		return ElasticsearchService.getAllType(indexName);
	}

	@RequestMapping("/getTypeMapping")
	@ResponseBody
	public Map<String, Object> getTypeMapping() {
		return ElasticsearchService.getAllTypeMapping(indexName);
	}

	@RequestMapping("/getAllIndexAndType")
	@ResponseBody
	public Map<String, String[]> getAllIndexAndType() {
		return ElasticsearchService.getAllIndexAndType();
	}
	
	@RequestMapping("/getIndexDataCount")
	@ResponseBody
	public long getIndexDataCount() {
		return ElasticsearchService.getIndexDataCount(indexName);
	}
	
	@RequestMapping("/getIndexTypeDataCount")
	@ResponseBody
	public long getIndexTypeDataCount() {
		return ElasticsearchService.getIndexTypeDataCount(indexName,esType);
	}
	
	
	

	/**
	 * 插入记录
	 * 
	 * @return
	 */
	@RequestMapping("/insertJson")
	@ResponseBody
	public String insertJson() {
		String id =null;
		for(int i=50;i<100;i++){
			JSONObject jsonObject = new JSONObject();
			jsonObject.put("id",i);
			Random r=new Random();//产生随机数
			int age=20+r.nextInt(20);//获取20到40之间的随机数
			jsonObject.put("age", age);
			int name=10000+r.nextInt(20000);//获取10000到10000之间的随机数
			jsonObject.put("name", "j-" + name);
			StringBuffer sb=new StringBuffer();
			for(int m=0;m<4;m++){
				long result=Math.round(Math.random()*25+65);
		        //将ASCII码转换成字符
				sb.append(String.valueOf((char)result)) ;
			}
			jsonObject.put("msg", sb.toString());
			jsonObject.put("date", new Date());
			jsonObject.put("dateStr", DateUtil.dateStr_UTC_08(new Date()));
			id = ElasticsearchService.addJsonData(jsonObject, indexName, esType, jsonObject.getString("id"));
		System.out.println(id);
		}
			return id;
	}
	
	
	
	
	@RequestMapping("/insertJsonStr")
	@ResponseBody
	public void insertJsonStr() {
		for(int i=0;i<100;i++){
			JSONObject jsonObject = new JSONObject();
			jsonObject.put("id",i);
			Random r=new Random();//产生随机数
			int age=20+r.nextInt(20);//获取20到40之间的随机数
			jsonObject.put("age", age);
			int name=10000+r.nextInt(20000);//获取10000到10000之间的随机数
			jsonObject.put("name", "j-" + name);
			StringBuffer sb=new StringBuffer();
			for(int m=0;m<4;m++){
				long result=Math.round(Math.random()*25+65);
		        //将ASCII码转换成字符
				sb.append(String.valueOf((char)result)) ;
			}
			jsonObject.put("msg", sb.toString());
			jsonObject.put("date", new Date());
			jsonObject.put("dateStr", DateUtil.dateStr_UTC_08(new Date()));
			
			String jsonStr=FastJsonUtils.toJSONStringDateUTC8(jsonObject);
			System.out.println(ElasticsearchService.insertjsonObjectStr(indexName, esType,i+"", jsonStr));
			
		}
	}
	
	
	
	
	
	@RequestMapping("/insertJsonTest")
	@ResponseBody
	public String insertJsonTest() {
		String id =null;
		
			id = ElasticsearchService.addJsonDataTest( indexName, esType);
		return id;
	}

	/**
	 * 插入记录
	 * 
	 * @return
	 */
	@RequestMapping("/insertModel")
	@ResponseBody
	public String insertModel() {
		Author author = new Author();
		author.setId(1L);
		author.setName("tianshouzhi");
		author.setRemark("java developer");

		Tutorial tutorial = new Tutorial();
		tutorial.setId(1L);
		tutorial.setName("elastic search");

		Article article = new Article();
		article.setId(1L);
		article.setTitle("springboot integreate elasticsearch");
		article.setAbstracts("springboot integreate elasticsearch is very easy");
		article.setTutorial(tutorial);
		article.setAuthor(author);
		article.setContent("elasticsearch based on lucene," + "spring-data-elastichsearch based on elaticsearch"
				+ ",this tutorial tell you how to integrete springboot with spring-data-elasticsearch");
		article.setPostTime(new Date());
		article.setClickCount(1L);

		JSONObject jsonObject = (JSONObject) JSONObject.toJSON(article);
		String id = ElasticsearchService.addJsonData(jsonObject, indexName, esType, jsonObject.getString("id"));
		return id;
	}

	/**
	 * 删除记录
	 * 
	 * @return
	 */
	@RequestMapping("/delete")
	@ResponseBody
	public String delete(String id) {
		if (StringUtils.isNotBlank(id)) {
			ElasticsearchService.deleteDataById(indexName, esType, id);
			return "删除id=" + id;
		} else {
			return "id为空";
		}
	}
	
	@RequestMapping("/deleteBatchByIds")
	@ResponseBody
	public String deleteBatchByIds(String ids) {
		if (StringUtils.isNotBlank(ids)) {
			ElasticsearchService.deleteBatchByIds(indexName, esType, ids);
			return "批量删除ids=" + ids;
		} else {
			return "ids为空";
		}
	}
	
	

	/**
	 * 更新数据
	 * 
	 * @return
	 */
	@RequestMapping("/update")
	@ResponseBody
	public String update(String id) {
		if (StringUtils.isNotBlank(id)) {
			JSONObject jsonObject = new JSONObject();
			jsonObject.put("id", id);
			jsonObject.put("age", 31);
			jsonObject.put("name", "修改");
			jsonObject.put("date", new Date());
			ElasticsearchService.updateJsonObjectById(jsonObject, indexName, esType, id);
			return "id=" + id;
		} else {
			return "id为空";
		}
	}

	/**
	 * 获取数据
	 * 
	 * @param id
	 * @return
	 */
	@RequestMapping("/getDataById")
	@ResponseBody
	public Object getDataById(String id) {
		if (StringUtils.isNotBlank(id)) {
			Map<String, Object> map = ElasticsearchService.searchDataById(indexName, esType, id, null);
			if(map==null){
				return "没有查询到数据";
			}else{
				return map;
			}
			
		} else {
			return "id为空";
		}
	}

	/**
	 * 查询数据 模糊查询
	 * 
	 * @return
	 */
	@RequestMapping("/queryMatchData")
	@ResponseBody
	public List<Map<String, Object>> queryMatchData() {
		BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
		boolean matchPhrase = false;
		if (matchPhrase == Boolean.TRUE) {
			boolQuery.must(QueryBuilders.matchPhraseQuery("name", "修"));
		} else {
			boolQuery.must(QueryBuilders.matchQuery("name", "修"));
		}
		List<Map<String, Object>> list = ElasticsearchService.searchListData(indexName, esType, boolQuery, 10, null,
				null, null);
		return list;
	}

	/**
	 * 通配符查询数据 通配符查询 ?用来匹配1个任意字符，*用来匹配零个或者多个字符
	 * 
	 * @return
	 */
	@RequestMapping("/queryWildcardData")
	@ResponseBody
	public List<Map<String, Object>> queryWildcardData() {
		QueryBuilder queryBuilder = QueryBuilders.wildcardQuery("name.keyword","j-1*");
		List<Map<String, Object>> list = ElasticsearchService.searchListData(indexName, esType, queryBuilder, 100, null,
				null, null);
		return list;
	}

	/**
	 * 正则查询
	 * 
	 * @return
	 */
	@RequestMapping("/queryRegexpData")
	@ResponseBody
	public List<Map<String, Object>> queryRegexpData() {
		QueryBuilder queryBuilder = QueryBuilders.regexpQuery("name.keyword", "j--[0-9]{1,11}");
		List<Map<String, Object>> list = ElasticsearchService.searchListData(indexName, esType, queryBuilder, 10, null,
				null, null);
		return list;
	}

	/**
	 * 查询数字范围数据
	 * 
	 * @return
	 */
	@RequestMapping("/queryIntRangeData")
	@ResponseBody
	public List<Map<String, Object>> queryIntRangeData() {
		BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
		boolQuery.must(QueryBuilders.rangeQuery("age").from(21).to(25));
		List<Map<String, Object>> list = ElasticsearchService.searchListData(indexName, esType, boolQuery, 10, null,
				null, null);
		return list;
	}

	/**
	 * 查询日期范围数据
	 * 
	 * @return
	 */
	@RequestMapping("/queryDateRangeData")
	@ResponseBody
	public List<Map<String, Object>> queryDateRangeData() {
		BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
		boolQuery
				.must(QueryBuilders.rangeQuery("date").from("2018-04-25T08:33:44.840Z").to("2018-04-25T10:03:08.081Z"));
		List<Map<String, Object>> list = ElasticsearchService.searchListData(indexName, esType, boolQuery, 10, null,
				null, null);
		return list;
	}

	/**
	 * 查询分页
	 * 
	 * @param pageNum
	 *             第几页
	 * @param pageSize
	 *            每页大小
	 * @return
	 */
	@RequestMapping("/queryPage")
	@ResponseBody
	public PageInfo<Map<String, Object>> queryPage(String pageNum, String pageSize) {
		PageInfo<Map<String, Object>> pageInfo =null;
		if (StringUtils.isNotBlank(pageNum) && StringUtils.isNotBlank(pageSize)) {
			BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
			boolQuery.must(QueryBuilders.rangeQuery("id").from("0").to("100000"));
			boolQuery.must(QueryBuilders.termQuery("age","27"));
			//boolQuery.must(QueryBuilders.wildcardQuery("msg.keyword","M*"));
			//2018-07-25T03:12:29.637Z
			//boolQuery.must(QueryBuilders.rangeQuery("date").from("2018-07-25T17:00:00+08:00").to("2018-07-25T18:00:00+08:00"));
			boolQuery.must(QueryBuilders.rangeQuery("dateStr").from("2018-07-25+T17:00:00+08:00").to("2018-07-25T18:00:00+08:00"));
			/**
			//where条件 a=8 and (b=3 or b=4)
			boolQuery.must(QueryBuilders.termQuery("a","8"));
			boolQuery.must(QueryBuilders.boolQuery().should(QueryBuilders.termQuery("b","3"))
					.should(QueryBuilders.termQuery("b","4")));
					**/
			pageInfo = ElasticsearchService.searchDataPage(indexName, esType, Integer.parseInt(pageNum),
					Integer.parseInt(pageSize), boolQuery, null, "id","DESC",  null);
			
		} 
		return pageInfo;
	}
}