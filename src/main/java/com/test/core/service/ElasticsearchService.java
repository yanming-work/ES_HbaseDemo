package com.test.core.service;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSONObject;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.test.core.config.ElasticSearchConfig;
import com.test.core.result.PageInfo;
import com.test.core.util.DateUtil;
import com.test.core.util.FastJsonUtils;

@Component
public class ElasticsearchService {

	// Relational DB -> Databases -> Tables -> Rows -> Columns
	// Elasticsearch -> Indices -> Types -> Documents -> Fields
	// ES中可以有多个索引（index）（数据库），每个索引可以包含多个类型(type)（表），每个类型可以包含多个文档（document）行，然后每个文档可以包含多个字段（Field）（列）
	// es最终是如何存储Date类型的了。是的，它最终的输出方式都是以字符串输出，只是默认的格式是：1970-01-01T00:00:00Z
	// ，也就是默认的 UTC世界协调时间 格式，默认是ISO
	// 8601标准，例如2015-02-27T00:07Z(Z零时区)、2015-02-27T08:07+08:00(+08:00东八区)。直接使用Date类型存储时间是没问题的。但是需要注意的是，时区要少了8小时，我们要主动加上。

	private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchService.class);

	@Autowired
	private TransportClient transportClient;

	private static TransportClient client;

	private static int MAX_RESULT = 10000;
	// 每一万条提交一次，一次性提交的过少反而更慢
	private static int Batch_COUNT = 10000;
	// 默认UTC世界协调时间 格式，CST:中国标准时间
	public static boolean UTC_08_DATE = true;

	/**
	 * 初始化，创建链接
	 */
	/**
	 * static{ String hostName="192.168.1.5"; int port=9300; String
	 * clusterName="elasticsearch"; int poolSize=5; // 配置信息 Settings esSetting =
	 * Settings.builder().put("cluster.name", clusterName) // 集群名字
	 * .put("client.transport.sniff", true)// 增加嗅探机制，找到ES集群
	 * .put("thread_pool.search.size", poolSize)// 增加线程池个数，暂时设为5 .build(); //
	 * 创建client try { client = new
	 * PreBuiltTransportClient(esSetting).addTransportAddresses(new
	 * InetSocketTransportAddress(InetAddress.getByName(hostName), port)); }
	 * catch (UnknownHostException e) { e.printStackTrace(); }
	 * 
	 * }
	 **/

	/**
	 * @PostContruct是spring框架的注解 spring容器初始化的时候执行该方法
	 */
	@PostConstruct
	public void init() {
		client = this.transportClient;
		try {
			MAX_RESULT = Integer.valueOf(ElasticSearchConfig.maxResultWindow);
		} catch (Exception e) {
		}
	}

	/************************************
	 * index start
	 ******************************************/

	/**
	 * 创建索引
	 *
	 * @param index
	 * @return
	 */
	public static boolean createIndex(String index_name) {
		boolean flag = false;
		// index名必须全小写，否则报错
		if (index_name != null && !"".equals(index_name) && client != null) {
			index_name = index_name.trim().toLowerCase();// 转换成小写
			if (!isIndexExist(index_name)) {
				LOGGER.info("Index is not exits!");
			}
			CreateIndexResponse indexresponse = client.admin().indices().prepareCreate(index_name).execute()
					.actionGet();
			LOGGER.info("执行建立成功？" + indexresponse.isAcknowledged());
			flag = indexresponse.isAcknowledged();
		}
		return flag;
	}

	/**
	 * 删除索引
	 *
	 * @param index
	 * @return
	 */
	public static boolean deleteIndex(String index_name) {
		boolean flag = false;
		if (index_name != null && !"".equals(index_name) && client != null) {
			index_name = index_name.trim().toLowerCase();// 转换成小写
			if (!isIndexExist(index_name)) {
				LOGGER.info("Index is not exits!");
			}
			DeleteIndexResponse dResponse = client.admin().indices().prepareDelete(index_name).execute().actionGet();
			if (dResponse.isAcknowledged()) {
				LOGGER.info("delete index " + index_name + "  successfully!");
			} else {
				LOGGER.info("Fail to delete index " + index_name);
			}
			flag = dResponse.isAcknowledged();
		}
		return flag;
	}

	/**
	 * 判断索引是否存在
	 *
	 * @param index
	 * @return
	 */
	public static boolean isIndexExist(String index_name) {
		boolean flag = false;
		if (index_name != null && !"".equals(index_name) && client != null) {
			index_name = index_name.trim().toLowerCase();// 转换成小写
			IndicesExistsResponse inExistsResponse = client.admin().indices()
					.exists(new IndicesExistsRequest(index_name)).actionGet();
			if (inExistsResponse.isExists()) {
				LOGGER.info("Index [" + index_name + "] is exist!");
			} else {
				LOGGER.info("Index [" + index_name + "] is not exist!");
			}
			flag = inExistsResponse.isExists();
		}
		return flag;
	}

	/**
	 * 判断指定的索引的类型是否存在
	 * 
	 * @param index_name
	 *            索引名
	 * @param index_type
	 *            索引类型
	 * @return 存在：true; 不存在：false;
	 */
	public boolean isExistsType(String index_name, String index_type) {
		boolean flag = false;
		try {
			if (index_name != null && !"".equals(index_name) && client != null) {
				index_name = index_name.trim().toLowerCase();// 转换成小写
				TypesExistsResponse response = client.admin().indices()
						.typesExists(new TypesExistsRequest(new String[] { index_name }, index_type)).actionGet();
				flag = response.isExists();
			}
		} catch (Exception e) {
			System.out.println("创建索引失败！" + e);
		}
		return flag;
	}

	/**
	 * 获取所有index
	 */
	public static Set<String> getAllIndices() {
		ActionFuture<IndicesStatsResponse> isr = client.admin().indices().stats(new IndicesStatsRequest().all());
		//Map<String, IndexStats> indexStatsMap = isr.actionGet().getIndices();
		Set<String> set = isr.actionGet().getIndices().keySet();
		return set;
	}

	public static String[] getAllType(String index_name) {
		String[] typeArr = null;
		try {
			if (index_name != null && !"".equals(index_name) && client != null) {
				index_name = index_name.trim().toLowerCase();// 转换成小写
				IndexMetaData indexMetaData = client.admin().cluster().prepareState().execute().actionGet().getState()
						.getMetaData().getIndices().get(index_name);
				if (indexMetaData != null) {
					ImmutableOpenMap<String, MappingMetaData> mappings = indexMetaData.getMappings();
					typeArr = new String[mappings.size()];
					int i = 0;
					for (ObjectObjectCursor<String, MappingMetaData> cursor : mappings) {
						typeArr[i] = cursor.key;
						i++;
					}
				}

			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return typeArr;
	}

	public static Map<String, Object> getAllTypeMapping(String index_name) {
		Map<String, Object> map = null;
		try {
			if (index_name != null && !"".equals(index_name) && client != null) {
				index_name = index_name.trim().toLowerCase();// 转换成小写
				IndexMetaData indexMetaData = client.admin().cluster().prepareState().execute().actionGet().getState()
						.getMetaData().getIndices().get(index_name);
				if (indexMetaData != null) {
					ImmutableOpenMap<String, MappingMetaData> mappings = indexMetaData.getMappings();
					map = new HashMap<String, Object>();
					for (ObjectObjectCursor<String, MappingMetaData> cursor : mappings) {
						System.out.println(cursor.key); // 索引下的每个type
						System.out.println(cursor.value.getSourceAsMap()); // 每个type的mapping
						map.put(cursor.key, cursor.value.getSourceAsMap());
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return map;
	}

	public static Map<String, String[]> getAllIndexAndType() {
		Map<String, String[]> map = new HashMap<String, String[]>();
		ActionFuture<IndicesStatsResponse> isr = client.admin().indices().stats(new IndicesStatsRequest().all());
		Set<String> set = isr.actionGet().getIndices().keySet();
		for (String index_name : set) {
			// System.out.println(index_name);
			IndexMetaData indexMetaData = client.admin().cluster().prepareState().execute().actionGet().getState()
					.getMetaData().getIndices().get(index_name);
			String[] typeArr = null;
			if (indexMetaData != null) {
				ImmutableOpenMap<String, MappingMetaData> mappings = indexMetaData.getMappings();
				typeArr = new String[mappings.size()];
				int i = 0;
				for (ObjectObjectCursor<String, MappingMetaData> cursor : mappings) {
					typeArr[i] = cursor.key;
					i++;
				}
			}

			map.put(index_name, typeArr);
		}

		return map;
	}

	/**
	 * 关闭索引
	 * 
	 * @param index
	 * @return
	 */
	public static boolean closeIndex(String index_name) {
		boolean flag = false;
		try {
			if (index_name != null && !"".equals(index_name) && client != null) {
				index_name = index_name.trim().toLowerCase();// 转换成小写
				CloseIndexResponse response = client.admin().indices().prepareClose(index_name).get();
				flag = response.isAcknowledged();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return flag;
	}

	/**
	 * 打开索引
	 * 
	 * @param index
	 * @return
	 */
	public static boolean openIndex(String index_name) {
		boolean flag = false;
		try {
			if (index_name != null && !"".equals(index_name) && client != null) {
				index_name = index_name.trim().toLowerCase();// 转换成小写
				OpenIndexResponse response = client.admin().indices().prepareOpen(index_name).get();
				flag = response.isAcknowledged();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return flag;
	}

	/**
	 * 
	 * @Description: 重构索引(更新词库之后)
	 * 
	 */
	@SuppressWarnings("deprecation")
	public void reindex(String index_name, int size) {
		try {
			if (index_name != null && !"".equals(index_name) && client != null) {
				index_name = index_name.trim().toLowerCase();// 转换成小写
				if (size <= 0) {
					size = 100;
				}
				if (size > MAX_RESULT) {
					size = MAX_RESULT;
				}

				SearchResponse scrollResp = client.prepareSearch(index_name)//
						.setScroll(new TimeValue(60000))//
						.setQuery(QueryBuilders.matchAllQuery())//
						.setSize(size).get(); // max of 100 hits will be
												// returned for
				// Scroll until no hits are returned
				do {
					for (SearchHit hit : scrollResp.getHits().getHits()) {
						client.prepareIndex(index_name, hit.getType(), hit.getId()).setSource(hit.getSourceAsString())
								.execute().actionGet();
					}
					scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000))
							.execute().actionGet();
				} while (scrollResp.getHits().getHits().length != 0);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 索引统计
	 * 
	 * @param client
	 * @param index
	 */
	public static void indexStats(String index_name) {
		try {
			if (index_name != null && !"".equals(index_name) && client != null) {
				index_name = index_name.trim().toLowerCase();// 转换成小写
				IndicesStatsResponse response = client.admin().indices().prepareStats(index_name).all().get();
				ShardStats[] shardStatsArray = response.getShards();
				for (ShardStats shardStats : shardStatsArray) {
					System.out.println("shardStats {}:" + shardStats.toString());
				}
				Map<String, IndexStats> indexStatsMap = response.getIndices();
				for (String key : indexStatsMap.keySet()) {
					System.out.println("indexStats {}:" + indexStatsMap.get(key));
				}
				CommonStats commonStats = response.getTotal();
				System.out.println("total commonStats {}:" + commonStats.toString());
				commonStats = response.getPrimaries();
				System.out.println("primaries commonStats {}:" + commonStats.toString());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static long getIndexDataCount(String index_name) {

		String response = client.prepareSearch(index_name).setSize(0).execute().actionGet().toString();

		JSONObject JsonResponse = JSONObject.parseObject(response);
		JSONObject JsonResponse_hits = JSONObject.parseObject(JsonResponse.get("hits").toString());
		String count_num = JsonResponse_hits.get("total").toString();

		// System.out.println(JsonResponse.get("hits"));

		return Long.valueOf(count_num);

	}

	public static long getIndexTypeDataCount(String index_name, String index_type) {
		String response = client.prepareSearch(index_name).setTypes(index_type).setSize(0).execute().actionGet()
				.toString();

		JSONObject JsonResponse = JSONObject.parseObject(response);
		JSONObject JsonResponse_hits = JSONObject.parseObject(JsonResponse.get("hits").toString());
		String count_num = JsonResponse_hits.get("total").toString();

		// System.out.println(JsonResponse.get("hits"));

		return Long.valueOf(count_num);

	}

	/************************************
	 * index end
	 ******************************************/

	/************************************
	 * save start
	 ******************************************/

	/**
	 * 数据添加，正定ID
	 *
	 * @param jsonObject
	 *            要增加的数据
	 * @param index
	 *            索引，类似数据库
	 * @param type
	 *            类型，类似表
	 * @param id
	 *            数据ID
	 * @return
	 */
	public static String addJsonData(JSONObject jsonObject, String index_name, String type, String id) {
		String return_id = null;
		if (index_name != null && !"".equals(index_name) && client != null) {
			index_name = index_name.trim().toLowerCase();// 转换成小写

			if (UTC_08_DATE) {
				IndexResponse response = client.prepareIndex(index_name, type, id)
						.setSource(FastJsonUtils.toJSONNoFeaturesDateUTC8(jsonObject), XContentType.JSON).get();
				// LOGGER.info("addData response status:{},id:{}",
				// response.status().getStatus(), response.getId());
				return_id = response.getId();
			} else {
				IndexResponse response = client.prepareIndex(index_name, type, id).setSource(jsonObject).get();
				// LOGGER.info("addData response status:{},id:{}",
				// response.status().getStatus(), response.getId());
				return_id = response.getId();
			}

		}
		return return_id;
	}

	public static String addJsonDataTest(String index_name, String index_type) {
		if (index_name != null && !"".equals(index_name) && client != null) {
			index_name = index_name.trim().toLowerCase();// 转换成小写

			// 开启批量插入
			int count = 0;
			BulkRequestBuilder bulkRequest = client.prepareBulk();
			for (int i = 400000; i < 500000; i++) {
				// 业务对象
				JSONObject jsonObject = new JSONObject();
				jsonObject.put("id", i);
				Random r = new Random();// 产生随机数
				int age = 20 + r.nextInt(20);// 获取20到40之间的随机数
				jsonObject.put("age", age);
				int name = 10000 + r.nextInt(20000);// 获取10000到10000之间的随机数
				jsonObject.put("name", "j-" + name);
				StringBuffer sb = new StringBuffer();
				for (int m = 0; m < 4; m++) {
					long result = Math.round(Math.random() * 25 + 65);
					// 将ASCII码转换成字符
					sb.append(String.valueOf((char) result));
				}
				jsonObject.put("msg", sb.toString());
				jsonObject.put("date", new Date());
				jsonObject.put("dateStr", DateUtil.date(new Date()));

				if (UTC_08_DATE) {
					IndexRequestBuilder indexRequest = client.prepareIndex(index_name, index_type)
							// 指定不重复的ID
							.setSource(FastJsonUtils.toJSONNoFeaturesDateUTC8(jsonObject), XContentType.JSON)
							.setId(String.valueOf(i));

					// 添加到builder中
					bulkRequest.add(indexRequest);
				} else {

					IndexRequestBuilder indexRequest = client.prepareIndex(index_name, index_type)
							// 指定不重复的ID
							.setSource(jsonObject).setId(String.valueOf(i));

					// 添加到builder中
					bulkRequest.add(indexRequest);
				}

				if (count % Batch_COUNT == 0 || count == 100000) {
					bulkRequest.execute().actionGet();
					System.out.println("提交了：" + count);
				}
				count++;

			}

			BulkResponse bulkResponse = bulkRequest.execute().actionGet();
			if (bulkResponse.hasFailures()) {
				// process failures by iterating through each bulk response item
				System.out.println(bulkResponse.buildFailureMessage());
			}
			System.out.println("批量插入完毕");

		}
		return "批量插入完毕";
	}

	/**
	 * 数据添加
	 *
	 * @param jsonObject
	 *            要增加的数据
	 * @param index
	 *            索引，类似数据库
	 * @param type
	 *            类型，类似表
	 * @return
	 */
	public static String addJsonData(JSONObject jsonObject, String index_name, String type) {
		String return_id = null;
		if (index_name != null && !"".equals(index_name) && client != null) {
			index_name = index_name.trim().toLowerCase();// 转换成小写
			return_id = addJsonData(jsonObject, index_name, type,
					UUID.randomUUID().toString().replaceAll("-", "").toUpperCase());
		}
		return return_id;
	}

	/**
	 * 保存MAP
	 * 
	 * @Title : saveMap
	 * @功能描述:
	 * @设定文件：@param index_name
	 * @设定文件：@param index_type
	 * @设定文件：@param id
	 * @设定文件：@param sourceMap
	 * @设定文件：@return
	 * @返回类型：boolean
	 * @throws ：
	 */
	public static boolean saveMap(String index_name, String index_type, String id, Map<String, Object> sourceMap) {
		boolean flag = false;
		try {
			if (index_name != null && !"".equals(index_name) && client != null) {
				index_name = index_name.trim().toLowerCase();// 转换成小写
				if (id != null && !"".equals(id)) {

					if (UTC_08_DATE) {
						client.prepareIndex(index_name, index_type).setId(id)
								.setSource(FastJsonUtils.toJSONNoFeaturesDateUTC8(sourceMap), XContentType.JSON).get();
					} else {
						client.prepareIndex(index_name, index_type).setId(id).setSource(sourceMap).get();
					}

					flag = true;
				} else {
					flag = saveMap(index_name, index_type, sourceMap);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return flag;
	}

	/**
	 * 保存MAP
	 * 
	 * @Title : saveMap
	 * @功能描述:
	 * @设定文件：@param index_name
	 * @设定文件：@param index_type
	 * @设定文件：@param sourceMap
	 * @设定文件：@return
	 * @返回类型：boolean
	 * @throws ：
	 */
	public static boolean saveMap(String index_name, String index_type, Map<String, Object> sourceMap) {
		boolean flag = false;
		try {
			if (index_name != null && !"".equals(index_name) && client != null) {
				index_name = index_name.trim().toLowerCase();// 转换成小写

				if (UTC_08_DATE) {
					client.prepareIndex(index_name, index_type)
							.setSource(FastJsonUtils.toJSONNoFeaturesDateUTC8(sourceMap), XContentType.JSON).get();
				} else {
					client.prepareIndex(index_name, index_type).setSource(sourceMap).get();
				}
				flag = true;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return flag;
	}

	/**
	 * 保存MAP
	 * 
	 * @Title : saveMap
	 * @功能描述:
	 * @设定文件：@param index_name
	 * @设定文件：@param index_type
	 * @设定文件：@param sourceMap
	 * @设定文件：@return
	 * @返回类型：boolean
	 * @throws ：
	 */
	public static String saveMapReturnId(String index_name, String index_type, Map<String, Object> sourceMap) {
		String id = null;
		try {
			if (index_name != null && !"".equals(index_name) && client != null) {
				index_name = index_name.trim().toLowerCase();// 转换成小写

				IndexResponse response = null;
				if (UTC_08_DATE) {
					response = client.prepareIndex(index_name, index_type)
							.setSource(FastJsonUtils.toJSONNoFeaturesDateUTC8(sourceMap), XContentType.JSON).get();
				} else {
					response = client.prepareIndex(index_name, index_type).setSource(sourceMap).get();
				}
				if (response != null) {
					id = response.getId();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return id;
	}

	/**
	 * 保存 jsonObjectStr
	 * 
	 * @Title : insertjsonObjectStr
	 * @功能描述:
	 * @设定文件：@param index_name
	 * @设定文件：@param index_type
	 * @设定文件：@param id
	 * @设定文件：@param jsonObjectStr
	 * @设定文件：@return
	 * @返回类型：long
	 * @throws ：
	 */
	public static boolean insertjsonObjectStr(String index_name, String index_type, String id, String jsonObjectStr) {
		boolean flag = false;
		if (index_name != null && !"".equals(index_name) && index_type != null && !"".equals(index_type)) {
			try {
				if (index_name != null && !"".equals(index_name) && client != null) {
					index_name = index_name.trim().toLowerCase();// 转换成小写
					if (id != null && !"".equals(id)) {
						
						if (UTC_08_DATE) {
							client.prepareIndex(index_name, index_type, id).setSource(FastJsonUtils.toJSONNoFeaturesDateUTC8(FastJsonUtils.jsonStrToMap(jsonObjectStr)), XContentType.JSON).get();
						}else{
							client.prepareIndex(index_name, index_type, id).setSource(jsonObjectStr, XContentType.JSON).get();
						}

						flag = true;
						/**
						 *
						 * IndexResponse response
						 * =client.prepareIndex(index_name,
						 * index_type,id).setSource(jsonObjectStr,
						 * XContentType.JSON).get(); System.out.println("索引名称："
						 * + response.getIndex()); System.out.println("类型：" +
						 * response.getType()); System.out.println("文档ID：" +
						 * response.getId()); // 第一次使用是1
						 * System.out.println("当前实例状态：" + response.status());
						 */

					} else {
						flag = insertjsonObjectStr(index_name, index_type, jsonObjectStr);

					}
				}

			} catch (Exception e) {
				e.printStackTrace();
			}

		}
		return flag;
	}

	/**
	 * 保存 jsonObjectStr
	 * 
	 * @Title : insertjsonObjectStr
	 * @功能描述:
	 * @设定文件：@param index_name
	 * @设定文件：@param index_type
	 * @设定文件：@param jsonObjectStr
	 * @设定文件：@return
	 * @返回类型：long
	 * @throws ：
	 */
	public static boolean insertjsonObjectStr(String index_name, String index_type, String jsonObjectStr) {
		boolean flag = false;
		if (index_name != null && !"".equals(index_name) && index_type != null && !"".equals(index_type)) {
			try {
				if (index_name != null && !"".equals(index_name) && client != null) {
					index_name = index_name.trim().toLowerCase();// 转换成小写
					
					if (UTC_08_DATE) {
						client.prepareIndex(index_name, index_type).setSource(FastJsonUtils.toJSONNoFeaturesDateUTC8(FastJsonUtils.jsonStrToMap(jsonObjectStr)), XContentType.JSON).get();
					}else{
						client.prepareIndex(index_name, index_type).setSource(jsonObjectStr, XContentType.JSON).get();
					}
				
					
					

					/**
					 *
					 * IndexResponse response =client.prepareIndex(index_name,
					 * index_type,id).setSource(jsonObjectStr,
					 * XContentType.JSON).get(); System.out.println("索引名称：" +
					 * response.getIndex()); System.out.println("类型：" +
					 * response.getType()); System.out.println("文档ID：" +
					 * response.getId()); // 第一次使用是1
					 * System.out.println("当前实例状态：" + response.status());
					 */
					flag = true;
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return flag;
	}

	/**
	 * 批量添加
	 * 
	 * @param index_name
	 * @param index_type
	 * @param mapjson
	 * @return
	 */
	public static boolean insertjsonObjectStrMap(String index_name, String index_type, Map<String, String> mapjson) {
		boolean flag = false;
		if (index_name != null && !"".equals(index_name) && index_type != null && !"".equals(index_type)) {
			try {
				if (mapjson != null) {
					if (index_name != null && !"".equals(index_name) && client != null) {
						index_name = index_name.trim().toLowerCase();// 转换成小写
						for (Map.Entry<String, String> entry : mapjson.entrySet()) {
							String id = entry.getKey();
							String jsonObjectStr = entry.getValue();
							if (id != null && !"".equals(id)) {
								insertjsonObjectStr(index_name, index_type, id, jsonObjectStr);
								/**
								 *
								 * IndexResponse response
								 * =client.prepareIndex(index_name,
								 * index_type,id).setSource(jsonObjectStr,
								 * XContentType.JSON).get();
								 * System.out.println("索引名称：" +
								 * response.getIndex());
								 * System.out.println("类型：" +
								 * response.getType());
								 * System.out.println("文档ID：" +
								 * response.getId()); // 第一次使用是1
								 * System.out.println("当前实例状态：" +
								 * response.status());
								 */

							} else {
								flag = insertjsonObjectStr(index_name, index_type, jsonObjectStr);
							}
							flag = true;
						}

					}

				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return flag;
	}

	/**
	 * 保存JSONObject
	 * 
	 * @Title : insertJSONObject
	 * @功能描述:
	 * @设定文件：@param index_name
	 * @设定文件：@param index_type
	 * @设定文件：@param id
	 * @设定文件：@param jsonObject
	 * @设定文件：@return
	 * @返回类型：long
	 * @throws ：
	 */
	public static boolean insertJSONObject(String index_name, String index_type, String id, JSONObject jsonObject) {
		boolean flag = false;
		if (index_name != null && !"".equals(index_name) && index_type != null && !"".equals(index_type)) {
			try {
				if (index_name != null && !"".equals(index_name) && client != null) {
					index_name = index_name.trim().toLowerCase();// 转换成小写
					if (id != null && !"".equals(id)) {

						if (UTC_08_DATE) {

							client.prepareIndex(index_name, index_type, id)
									.setSource(FastJsonUtils.toJSONNoFeaturesDateUTC8(jsonObject), XContentType.JSON)
									.get();
						} else {
							client.prepareIndex(index_name, index_type, id)
									.setSource(jsonObject.toJSONString(), XContentType.JSON).get();
						}
						flag = true;
					} else {
						flag = insertJSONObject(index_name, index_type, jsonObject);
					}

				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return flag;
	}

	/**
	 * 保存JSONObject
	 * 
	 * @Title : insertJSONObject
	 * @功能描述:
	 * @设定文件：@param index_name
	 * @设定文件：@param index_type
	 * @设定文件：@param jsonObject
	 * @设定文件：@return
	 * @返回类型：long
	 * @throws ：
	 */
	public static boolean insertJSONObject(String index_name, String index_type, JSONObject jsonObject) {
		boolean flag = false;
		if (index_name != null && !"".equals(index_name) && index_type != null && !"".equals(index_type)) {
			try {
				if (index_name != null && !"".equals(index_name) && client != null) {
					index_name = index_name.trim().toLowerCase();// 转换成小写
					if (UTC_08_DATE) {

						client.prepareIndex(index_name, index_type)
								.setSource(FastJsonUtils.toJSONNoFeaturesDateUTC8(jsonObject), XContentType.JSON).get();
					} else {
						client.prepareIndex(index_name, index_type)
								.setSource(jsonObject.toJSONString(), XContentType.JSON).get();
					}
					flag = true;
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return flag;
	}

	/**
	 * 保存Obj
	 * 
	 * @Title : insertObject
	 * @功能描述:
	 * @设定文件：@param index_name
	 * @设定文件：@param index_type
	 * @设定文件：@param id
	 * @设定文件：@param object
	 * @设定文件：@return
	 * @返回类型：boolean
	 * @throws ：
	 */
	public static boolean insertObject(String index_name, String index_type, String id, Object object) {
		boolean flag = false;
		if (index_name != null && !"".equals(index_name) && index_type != null && !"".equals(index_type)
				&& object != null) {
			try {
				if (index_name != null && !"".equals(index_name) && client != null) {
					index_name = index_name.trim().toLowerCase();// 转换成小写

					String jsonObjectStr = null;
					if (UTC_08_DATE) {
						jsonObjectStr = FastJsonUtils.toJSONNoFeaturesDateUTC8(object);
					} else {
						jsonObjectStr = JSONObject.toJSONString(object);
					}

					if (jsonObjectStr != null) {
						if (id != null && !"".equals(id)) {
							flag = insertjsonObjectStr(index_name, index_type, id, jsonObjectStr);
						} else {
							flag = insertjsonObjectStr(index_name, index_type, jsonObjectStr);
						}
					}

				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return flag;
	}

	/**
	 * 保存Obj
	 * 
	 * @Title : insertObject
	 * @功能描述:
	 * @设定文件：@param index_name
	 * @设定文件：@param index_type
	 * @设定文件：@param id
	 * @设定文件：@param object
	 * @设定文件：@return
	 * @返回类型：boolean
	 * @throws ：
	 */
	public static boolean insertObject(String index_name, String index_type, Object object) {
		boolean flag = false;
		if (index_name != null && !"".equals(index_name) && index_type != null && !"".equals(index_type)
				&& object != null) {
			try {
				if (index_name != null && !"".equals(index_name) && client != null) {
					index_name = index_name.trim().toLowerCase();// 转换成小写
					String jsonObjectStr = null;
					if (UTC_08_DATE) {
						jsonObjectStr = FastJsonUtils.toJSONNoFeaturesDateUTC8(object);
					} else {
						jsonObjectStr = JSONObject.toJSONString(object);
					}
					if (jsonObjectStr != null) {
						flag = insertjsonObjectStr(index_name, index_type, jsonObjectStr);
					}

				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return flag;
	}

	/**
	 * 添加List
	 * 
	 * @param index_name
	 * @param index_type
	 * @param list
	 * @return
	 */
	public static boolean insertList(String index_name, String index_type, List<?> list) {
		boolean flag = false;
		if (index_name != null && !"".equals(index_name) && index_type != null && !"".equals(index_type) && list != null
				&& list.size() > 0) {
			try {
				if (index_name != null && !"".equals(index_name) && client != null) {
					index_name = index_name.trim().toLowerCase();// 转换成小写

					for (Object object : list) {
						String jsonObjectStr = null;
						if (UTC_08_DATE) {
							jsonObjectStr = FastJsonUtils.toJSONNoFeaturesDateUTC8(object);
						} else {
							jsonObjectStr = JSONObject.toJSONString(object);
						}
						if (jsonObjectStr != null) {
							flag = flag || insertjsonObjectStr(index_name, index_type, jsonObjectStr);
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return flag;
	}

	public static boolean insertListBatch(String index_name, String index_type, List<?> list, String id_name) {
		boolean flag = false;
		if (index_name != null && !"".equals(index_name) && index_type != null && !"".equals(index_type) && list != null
				&& list.size() > 0) {
			try {
				if (index_name != null && !"".equals(index_name) && client != null) {
					index_name = index_name.trim().toLowerCase();// 转换成小写
					// 开启批量插入
					int count = 0;
					BulkRequestBuilder bulkRequest = client.prepareBulk();
					for (Object object : list) {
						String jsonObjectStr = null;
						if (UTC_08_DATE) {
							jsonObjectStr = FastJsonUtils.toJSONNoFeaturesDateUTC8(object);
						} else {
							jsonObjectStr = JSONObject.toJSONString(object);
						}
						if (jsonObjectStr != null) {
							IndexRequestBuilder indexRequest = null;
							if (id_name != null) {
								JSONObject jsonObject = JSONObject.parseObject(jsonObjectStr);
								if (jsonObject != null) {
									if (jsonObject.get(id_name) != null) {
										indexRequest = client.prepareIndex(index_name, index_type)
												// 指定不重复的ID
												.setSource(jsonObject).setId(jsonObject.get(id_name).toString());
									} else {
										indexRequest = client.prepareIndex(index_name, index_type)
												// 指定不重复的ID
												.setSource(jsonObject);
									}
								}

							} else {
								indexRequest = client.prepareIndex(index_name, index_type)
										// 指定不重复的ID
										.setSource(jsonObjectStr, XContentType.JSON);
							}

							if (indexRequest != null) {
								// 添加到builder中
								bulkRequest.add(indexRequest);
							}
						}

						if (count % Batch_COUNT == 0 || count == 100000) {
							bulkRequest.execute().actionGet();
							System.out.println("提交了：" + count);
						}
						count++;

					}

					BulkResponse bulkResponse = bulkRequest.execute().actionGet();
					if (bulkResponse.hasFailures()) {
						// process failures by iterating through each bulk
						// response item
						System.out.println(bulkResponse.buildFailureMessage());
					}
					System.out.println("批量插入完毕");

				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return flag;
	}

	/************************************
	 * save end
	 ******************************************/

	/************************************
	 * update start
	 ******************************************/

	/**
	 * 修改 jsonObjectStr
	 * 
	 * @Title : updatejsonObjectStr
	 * @功能描述:
	 * @设定文件：@param index_name
	 * @设定文件：@param index_type
	 * @设定文件：@param id
	 * @设定文件：@param jsonObjectStr
	 * @设定文件：@return
	 * @返回类型：long
	 * @throws ：
	 */
	public static boolean updatejsonObjectStrById(String index_name, String index_type, String id, String jsonObjectStr) {
		boolean flag = false;
		if (index_name != null && !"".equals(index_name) && index_type != null && !"".equals(index_type) && id != null
				&& !"".equals(id)) {
			try {
				if (index_name != null && !"".equals(index_name) && client != null) {
					index_name = index_name.trim().toLowerCase();// 转换成小写
					// UpdateResponse response =
					
					if (UTC_08_DATE) {
						client.prepareUpdate(index_name, index_type, id).setDoc(FastJsonUtils.toJSONNoFeaturesDateUTC8(FastJsonUtils.jsonStrToMap(jsonObjectStr)), XContentType.JSON).get();
					}else{
						client.prepareUpdate(index_name, index_type, id).setDoc(jsonObjectStr, XContentType.JSON).get();
					}
					
					
					flag = true;

					/**
					 * UpdateResponse response =
					 * client.prepareUpdate(index_name,
					 * index_type,id).setDoc(jsonObjectStr, XContentType.JSON).get();
					 * System.out.println("索引名称：" + response.getIndex());
					 * System.out.println("类型：" + response.getType());
					 * System.out.println("文档ID：" + response.getId()); //
					 * 第一次使用是1 System.out.println("当前实例状态：" +
					 * response.status()); //多次index这个版本号会变
					 * System.out.println("response.version:"+responseVersion);
					 */
				}

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return flag;
	}

	/**
	 * 修改JSONObject
	 * 
	 * @Title : updateJSONObject
	 * @功能描述:
	 * @设定文件：@param index_name
	 * @设定文件：@param index_type
	 * @设定文件：@param id
	 * @设定文件：@param jsonObject
	 * @设定文件：@return
	 * @返回类型：long
	 * @throws ：
	 */
	public static boolean updateJSONObjectById(String index_name, String index_type, String id, JSONObject jsonObject) {
		boolean flag = false;
		if (index_name != null && !"".equals(index_name) && index_type != null && !"".equals(index_type) && id != null
				&& !"".equals(id)) {
			try {
				if (index_name != null && !"".equals(index_name) && client != null) {
					index_name = index_name.trim().toLowerCase();// 转换成小写
					if (UTC_08_DATE) {
						client.prepareUpdate(index_name, index_type, id)
								.setDoc(FastJsonUtils.toJSONNoFeaturesDateUTC8(jsonObject), XContentType.JSON).get()
								.getVersion();
					} else {
						client.prepareUpdate(index_name, index_type, id).setDoc(jsonObject, XContentType.JSON).get()
								.getVersion();
					}

					flag = true;
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return flag;
	}

	/**
	 * 修改Object
	 * 
	 * @Title : updateObject
	 * @功能描述:
	 * @设定文件：@param index_name
	 * @设定文件：@param index_type
	 * @设定文件：@param id
	 * @设定文件：@param jsonObject
	 * @设定文件：@return
	 * @返回类型：long
	 * @throws ：
	 */
	public static boolean updateObjectById(String index_name, String index_type, String id, Object object) {
		boolean flag = false;
		if (index_name != null && !"".equals(index_name) && index_type != null && !"".equals(index_type) && id != null
				&& !"".equals(id)) {
			try {
				if (index_name != null && !"".equals(index_name) && client != null) {
					index_name = index_name.trim().toLowerCase();// 转换成小写

					String jsonObjectStr = null;
					if (UTC_08_DATE) {
						jsonObjectStr = FastJsonUtils.toJSONNoFeaturesDateUTC8(object);
					} else {
						jsonObjectStr = JSONObject.toJSONString(object);
					}
					if (jsonObjectStr != null) {
						flag = updatejsonObjectStrById(index_name, index_type, id, jsonObjectStr);
					}
				}

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return flag;
	}

	/**
	 * 通过ID 更新数据
	 *
	 * @param jsonObject
	 *            要增加的数据
	 * @param index
	 *            索引，类似数据库
	 * @param type
	 *            类型，类似表
	 * @param id
	 *            数据ID
	 * @return
	 */
	public static boolean updateJsonObjectById(JSONObject jsonObject, String index_name, String type, String id) {
		boolean flag = false;

		try {
			if (index_name != null && !"".equals(index_name) && client != null) {
				index_name = index_name.trim().toLowerCase();// 转换成小写

				UpdateRequest updateRequest = new UpdateRequest();
				if (UTC_08_DATE) {
					updateRequest.index(index_name).type(type).id(id)
							.doc(FastJsonUtils.toJSONNoFeaturesDateUTC8(jsonObject), XContentType.JSON);
				} else {
					updateRequest.index(index_name).type(type).id(id).doc(jsonObject);
				}
				client.update(updateRequest);
				// UpdateResponse u = client.update(updateRequest).get();
				flag = true;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return flag;
	}

	/************************************
	 * update end
	 ******************************************/

	/************************************
	 * delete start
	 ******************************************/

	/**
	 * 通过ID删除数据
	 *
	 * @param index
	 *            索引，类似数据库
	 * @param type
	 *            类型，类似表
	 * @param id
	 *            数据ID
	 */
	public static boolean deleteDataById(String index_name, String type, String id) {
		boolean flag = false;
		if (index_name != null && !"".equals(index_name) && client != null) {
			index_name = index_name.trim().toLowerCase();// 转换成小写
			DeleteResponse response = client.prepareDelete(index_name, type, id).execute().actionGet();

			LOGGER.info("deleteDataById response status:{},id:{}", response.status().getStatus(), response.getId());
			if (response.status().getStatus() == 200) {
				flag = true;
			}
		}
		return flag;
	}

	public static boolean deleteBatchByIds(String index_name, String index_type, String[] idArr) {
		boolean flag = false;
		if (idArr != null && idArr.length > 0) {
			// 开启批量插入
			int count = 0;
			BulkRequestBuilder bulkRequest = client.prepareBulk();
			for (int i = 0; i < idArr.length; i++) {

				DeleteRequestBuilder deleteRequestBuilder = client.prepareDelete(index_name, index_type, idArr[i]);
				// 添加到builder中
				bulkRequest.add(deleteRequestBuilder);

				if (count % Batch_COUNT == 0 || count == 100000) {
					bulkRequest.execute().actionGet();
					System.out.println("删除了：" + count);
				}
				count++;

			}

			BulkResponse bulkResponse = bulkRequest.execute().actionGet();
			if (bulkResponse.hasFailures()) {
				// process failures by iterating through each bulk response item
				System.out.println(bulkResponse.buildFailureMessage());
			}
			System.out.println("批量删除完毕");
			flag = true;
		}
		return flag;
	}

	public static boolean deleteBatchByIds(String index_name, String index_type, String ids) {
		boolean flag = false;
		if (ids != null && !"".equals(ids)) {
			String[] idArr = ids.split(",");
			if (idArr != null && idArr.length > 0) {
				// 开启批量插入
				int count = 0;
				BulkRequestBuilder bulkRequest = client.prepareBulk();
				for (int i = 0; i < idArr.length; i++) {

					DeleteRequestBuilder deleteRequestBuilder = client.prepareDelete(index_name, index_type, idArr[i]);
					// 添加到builder中
					bulkRequest.add(deleteRequestBuilder);

					if (count % Batch_COUNT == 0 || count == 100000) {
						bulkRequest.execute().actionGet();
						System.out.println("删除了：" + count);
					}
					count++;

				}

				BulkResponse bulkResponse = bulkRequest.execute().actionGet();
				if (bulkResponse.hasFailures()) {
					// process failures by iterating through each bulk response
					// item
					System.out.println(bulkResponse.buildFailureMessage());
				}
				System.out.println("批量删除完毕");
				flag = true;
			}
		}
		return flag;
	}

	/**
	 * 删除
	 * 
	 * @Title : deleteById
	 * @功能描述:
	 * @设定文件：@param index_name
	 * @设定文件：@param index_type
	 * @设定文件：@param id
	 * @返回类型：void
	 * @throws ：
	 */
	public static boolean deleteById(String index_name, String index_type, String id) {
		boolean flag = false;
		if (index_name != null && !"".equals(index_name) && index_type != null && !"".equals(index_type) && id != null
				&& !"".equals(id)) {
			try {
				if (index_name != null && !"".equals(index_name) && client != null) {
					index_name = index_name.trim().toLowerCase();// 转换成小写
					client.prepareDelete(index_name, index_type, id).get();
					flag = true;
					/**
					 * DeleteResponse response =
					 * client.prepareDelete(index_name, index_type,id).get();
					 * System.out.println("索引名称：" + response.getIndex());
					 * System.out.println("类型：" + response.getType());
					 * System.out.println("文档ID：" + response.getId());
					 * System.out.println("当前实例状态：" + response.status());
					 * //多次index这个版本号会变
					 * System.out.println("response.version:"+response.getVersion());
					 */
				}

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return flag;
	}

	/************************************
	 * delete end
	 ******************************************/

	/************************************
	 * select start
	 ******************************************/

	/**
	 * 通过ID获取数据
	 *
	 * @param index
	 *            索引，类似数据库
	 * @param type
	 *            类型，类似表
	 * @param id
	 *            数据ID
	 * @param fields
	 *            需要显示的字段，逗号分隔（缺省为全部字段）
	 * @return
	 */
	public static Map<String, Object> searchDataById(String index_name, String type, String id, String fields) {
		Map<String, Object> map = null;
		if (index_name != null && !"".equals(index_name) && client != null) {
			index_name = index_name.trim().toLowerCase();// 转换成小写
			GetRequestBuilder getRequestBuilder = client.prepareGet(index_name, type, id);

			if (StringUtils.isNotEmpty(fields)) {
				getRequestBuilder.setFetchSource(fields.split(","), null);
			}

			GetResponse getResponse = getRequestBuilder.execute().actionGet();

			map = getResponse.getSource();
		}
		return map;
	}

	/**
	 * 根据ID查询
	 * 
	 * @Title : selectById
	 * @功能描述:
	 * @设定文件：@param index_name
	 * @设定文件：@param index_type
	 * @设定文件：@param id
	 * @设定文件：@return
	 * @返回类型：String
	 * @throws ：
	 */
	public static String selectById(String index_name, String index_type, String id) {
		String resultStr = null;
		try {
			if (index_name != null && !"".equals(index_name) && client != null) {
				index_name = index_name.trim().toLowerCase();// 转换成小写
				GetResponse response = client.prepareGet(index_name, index_type, id).execute().actionGet();
				resultStr = response.getSourceAsString();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultStr;
	}

	public static Map<String, String> selectByKeyValue(String index_name, String index_type, String key, String value,
			int from, int size) {
		Map<String, String> jsonMap = null;
		try {
			if (index_name != null && !"".equals(index_name) && client != null) {
				index_name = index_name.trim().toLowerCase();// 转换成小写

				if (from < 0) {
					from = 0;
				}

				if (size < 0) {
					size = 10;
				}

				if ((from + size) > MAX_RESULT) {
					size = MAX_RESULT - from;
				}

				SearchRequestBuilder responsebuilder = client.prepareSearch(index_name).setTypes(index_type);
				SearchResponse myresponse = responsebuilder
						.setQuery(
								// 基本查询
								QueryBuilders.matchPhraseQuery(key, value))
						.setFrom(from).setSize(size)
						// .setExplain(true)
						.execute().actionGet();

				jsonMap = getHitsMap(myresponse.getHits());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return jsonMap;
	}

	public static Map<String, String> selectByKeyValueTop10(String index_name, String index_type, String key,
			String value) {
		Map<String, String> jsonMap = null;
		try {
			if (index_name != null && !"".equals(index_name) && client != null) {
				index_name = index_name.trim().toLowerCase();// 转换成小写
				jsonMap = selectByKeyValue(index_name, index_type, key, value, 0, 10);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return jsonMap;
	}

	public static Map<String, String> selectByKeyValuesTop10(String index_name, String index_type, String key,
			String... values) {
		Map<String, String> jsonMap = null;
		try {
			jsonMap = selectByKeyValues(index_name, index_type, key, 0, 10, values);

		} catch (Exception e) {
			e.printStackTrace();
		}
		return jsonMap;
	}

	public static Map<String, String> selectByKeyValues(String index_name, String index_type, String key, int from,
			int size, String... values) {
		Map<String, String> jsonMap = null;
		try {
			if (index_name != null && !"".equals(index_name) && client != null) {
				index_name = index_name.trim().toLowerCase();// 转换成小写
				SearchRequestBuilder responsebuilder = client.prepareSearch(index_name).setTypes(index_type);
				if (from < 0) {
					from = 0;
				}

				if (size < 0) {
					size = 10;
				}
				if ((from + size) > MAX_RESULT) {
					size = MAX_RESULT - from;
				}
				SearchResponse myresponse = responsebuilder
						.setQuery(
								// 多词条查询 ?
								// term主要用于精确匹配哪些值，比如数字，日期，布尔值或 not_analyzed
								// 的字符串(未经分析的文本数据类型)：
								QueryBuilders.termsQuery(key, values))
						.setFrom(from).setSize(size)
						// .setExplain(true)
						.execute().actionGet();

				jsonMap = getHitsMap(myresponse.getHits());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return jsonMap;
	}

	public static Map<String, String> selectAll(String index_name, String index_type, int from, int size) {
		Map<String, String> jsonMap = null;
		try {
			if (index_name != null && !"".equals(index_name) && client != null) {
				index_name = index_name.trim().toLowerCase();// 转换成小写
				SearchRequestBuilder responsebuilder = client.prepareSearch(index_name).setTypes(index_type);
				if (from < 0) {
					from = 0;
				}

				if (size < 0) {
					size = 10;
				}
				if ((from + size) > MAX_RESULT) {
					size = MAX_RESULT - from;
				}
				SearchResponse myresponse = responsebuilder
						.setQuery(
								// 3.查询全部
								QueryBuilders.matchAllQuery())
						.setFrom(from).setSize(size)
						// .setExplain(true)
						.execute().actionGet();

				jsonMap = getHitsMap(myresponse.getHits());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return jsonMap;
	}

	public static Map<String, String> selectAllTop100(String index_name, String index_type) {
		Map<String, String> jsonMap = null;
		try {
			if (index_name != null && !"".equals(index_name) && client != null) {
				index_name = index_name.trim().toLowerCase();// 转换成小写

				jsonMap = selectAll(index_name, index_type, 0, 100);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return jsonMap;
	}

	public static Map<String, String> selectCommonTerms(String index_name, String index_type, String key, String value,
			int from, int size) {
		Map<String, String> jsonMap = null;

		try {
			if (index_name != null && !"".equals(index_name) && client != null) {
				index_name = index_name.trim().toLowerCase();// 转换成小写
				if (from < 0) {
					from = 0;
				}

				if (size < 0) {
					size = 100;
				}
				if ((from + size) > MAX_RESULT) {
					size = MAX_RESULT - from;
				}

				SearchRequestBuilder responsebuilder = client.prepareSearch(index_name).setTypes(index_type);
				SearchResponse myresponse = responsebuilder
						.setQuery(
								// 4.常用词查询
								QueryBuilders.commonTermsQuery(key, value))
						.setFrom(from).setSize(size)
						// .setExplain(true)
						.execute().actionGet();

				jsonMap = getHitsMap(myresponse.getHits());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return jsonMap;
	}

	public static Map<String, String> selectCommonTermsTop100(String index_name, String index_type, String key,
			String value) {
		Map<String, String> jsonMap = null;

		try {

			jsonMap = selectCommonTerms(index_name, index_type, key, value, 0, 100);

		} catch (Exception e) {
			e.printStackTrace();
		}
		return jsonMap;
	}

	/**
	 * multiMatchQuery(Object text, String...
	 * fieldNames):text为文本值，fieldNames为字段名称。
	 * 举例说明：name、address为字段名称，13为文本值。查询name字段或者address字段文本值为13的结果集。
	 * 
	 * @return
	 */
	public static Map<String, String> selectByMultiMatchQuery(String index_name, String index_type, String text,
			int from, int size, String... fields) {
		Map<String, String> jsonMap = null;

		try {
			if (index_name != null && !"".equals(index_name) && client != null) {
				index_name = index_name.trim().toLowerCase();// 转换成小写
				if (from < 0) {
					from = 0;
				}

				if (size < 0) {
					size = 100;
				}
				if ((from + size) > MAX_RESULT) {
					size = MAX_RESULT - from;
				}
				SearchRequestBuilder responsebuilder = client.prepareSearch(index_name).setTypes(index_type);
				SearchResponse myresponse = responsebuilder
						.setQuery(
								// multiMatchQuery(text,fields)其中的fields是字段的名字，可以写好几个，每一个中间用逗号分隔
								QueryBuilders.multiMatchQuery(text, fields))
						.setFrom(from).setSize(size)
						// .setExplain(true)
						.execute().actionGet();

				jsonMap = getHitsMap(myresponse.getHits());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return jsonMap;
	}

	public static Map<String, String> selectByMultiMatchQueryTop100(String index_name, String index_type, String text,
			String... fields) {
		Map<String, String> jsonMap = null;

		try {
			jsonMap = selectByMultiMatchQuery(index_name, index_type, text, 0, 100, fields);

		} catch (Exception e) {
			e.printStackTrace();
		}
		return jsonMap;
	}

	/**
	 * 查询任意字段文本值为?的结果集。multi_match为指定某几个字段，query_string是查所有的字段。
	 * 
	 * @return
	 */
	public static Map<String, String> selectQuery(String index_name, String index_type, String text, int from,
			int size) {
		Map<String, String> jsonMap = null;

		try {
			if (index_name != null && !"".equals(index_name) && client != null) {
				index_name = index_name.trim().toLowerCase();// 转换成小写
				if (from < 0) {
					from = 0;
				}

				if (size < 0) {
					size = 100;
				}

				if ((from + size) > MAX_RESULT) {
					size = MAX_RESULT - from;
				}

				SearchRequestBuilder responsebuilder = client.prepareSearch(index_name).setTypes(index_type);
				SearchResponse myresponse = responsebuilder
						.setQuery(
								// query_string查询 （所有字段内容包含以下文本的）
								QueryBuilders.queryStringQuery(text))
						.setFrom(from).setSize(size)
						// .setExplain(true)
						.execute().actionGet();

				jsonMap = getHitsMap(myresponse.getHits());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return jsonMap;
	}

	public static Map<String, String> selectQueryTop100(String index_name, String index_type, String text) {
		Map<String, String> jsonMap = null;

		try {

			jsonMap = selectQuery(index_name, index_type, text, 0, 100);

		} catch (Exception e) {
			e.printStackTrace();
		}
		return jsonMap;
	}

	public static Map<String, String> selectSimpleQuery(String index_name, String index_type, String text, int from,
			int size) {
		Map<String, String> jsonMap = null;

		try {
			if (index_name != null && !"".equals(index_name) && client != null) {
				index_name = index_name.trim().toLowerCase();// 转换成小写
				if (from < 0) {
					from = 0;
				}

				if (size < 0) {
					size = 100;
				}
				if ((from + size) > MAX_RESULT) {
					size = MAX_RESULT - from;
				}

				SearchRequestBuilder responsebuilder = client.prepareSearch(index_name).setTypes(index_type);
				SearchResponse myresponse = responsebuilder
						.setQuery(
								// 8.simple_query_string查询,个人觉得与query_string没太大区别，感兴趣的可以深入研究
								QueryBuilders.simpleQueryStringQuery(text))
						.setFrom(from).setSize(size)
						// .setExplain(true)
						.execute().actionGet();

				jsonMap = getHitsMap(myresponse.getHits());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return jsonMap;
	}

	public static Map<String, String> selectSimpleQueryTop100(String index_name, String index_type, String text) {
		Map<String, String> jsonMap = null;

		try {

			jsonMap = selectSimpleQuery(index_name, index_type, text, 0, 100);

		} catch (Exception e) {
			e.printStackTrace();
		}
		return jsonMap;
	}

	/**
	 * 前缀查询
	 */
	public static Map<String, String> selectPrefixQuery(String index_name, String index_type, String key, String prefix,
			int from, int size) {
		Map<String, String> jsonMap = null;

		try {
			if (index_name != null && !"".equals(index_name) && client != null) {
				index_name = index_name.trim().toLowerCase();// 转换成小写
				if (from < 0) {
					from = 0;
				}

				if (size < 0) {
					size = 100;
				}
				if ((from + size) > MAX_RESULT) {
					size = MAX_RESULT - from;
				}

				SearchRequestBuilder responsebuilder = client.prepareSearch(index_name).setTypes(index_type);

				SearchResponse myresponse = responsebuilder
						.setQuery(
								// 8.前缀查询 （一个汉字，字符小写）
								// QueryBuilders.prefixQuery("name", "云")
								QueryBuilders.prefixQuery(key, prefix))
						.setFrom(from).setSize(size)
						// .setExplain(true)
						.execute().actionGet();

				jsonMap = getHitsMap(myresponse.getHits());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return jsonMap;
	}

	public static Map<String, String> selectPrefixQueryTop100(String index_name, String index_type, String key,
			String prefix) {
		Map<String, String> jsonMap = null;

		try {

			jsonMap = selectPrefixQuery(index_name, index_type, key, prefix, 0, 100);

		} catch (Exception e) {
			e.printStackTrace();
		}
		return jsonMap;
	}

	/**
	 * 使用模糊查询匹配文档查询
	 * 
	 * @param index_name
	 * @param index_type
	 * @param key
	 * @param fuzzy
	 * @return
	 */
	public static Map<String, String> selectFuzzyQuery(String index_name, String index_type, String key, String fuzzy,
			int from, int size) {
		Map<String, String> jsonMap = null;

		try {
			if (index_name != null && !"".equals(index_name) && client != null) {
				index_name = index_name.trim().toLowerCase();// 转换成小写
				if (from < 0) {
					from = 0;
				}

				if (size < 0) {
					size = 100;
				}
				if ((from + size) > MAX_RESULT) {
					size = MAX_RESULT - from;
				}

				SearchRequestBuilder responsebuilder = client.prepareSearch(index_name).setTypes(index_type);

				SearchResponse myresponse = responsebuilder
						.setQuery(
								// 9.fuzzy_like_this，fuzzy_like_this_field，fuzzy查询
								// fuzzyQuery:使用模糊查询匹配文档查询
								QueryBuilders.fuzzyQuery(key, fuzzy))
						.setFrom(from).setSize(size)
						// .setExplain(true)
						.execute().actionGet();

				jsonMap = getHitsMap(myresponse.getHits());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return jsonMap;
	}

	public static Map<String, String> selectFuzzyQueryTop100(String index_name, String index_type, String key,
			String fuzzy) {
		Map<String, String> jsonMap = null;

		try {
			jsonMap = selectFuzzyQuery(index_name, index_type, key, fuzzy, 0, 100);

		} catch (Exception e) {
			e.printStackTrace();
		}
		return jsonMap;
	}

	/**
	 * 通配符查询 ?用来匹配任意字符，*用来匹配零个或者多个字符
	 * 
	 * @param index_name
	 * @param index_type
	 * @param key
	 * @param wildcard
	 * @return
	 */
	public static Map<String, String> selectWildcardQuery(String index_name, String index_type, String key,
			String wildcard, int from, int size) {
		Map<String, String> jsonMap = null;

		try {
			if (index_name != null && !"".equals(index_name) && client != null) {
				index_name = index_name.trim().toLowerCase();// 转换成小写
				if (from < 0) {
					from = 0;
				}

				if (size < 0) {
					size = 100;
				}
				if ((from + size) > MAX_RESULT) {
					size = MAX_RESULT - from;
				}

				SearchRequestBuilder responsebuilder = client.prepareSearch(index_name).setTypes(index_type);

				SearchResponse myresponse = responsebuilder
						.setQuery(

								// 10.通配符查询 ?用来匹配任意字符，*用来匹配零个或者多个字符
								// QueryBuilders.wildcardQuery("name",
								// "岳*")//"?ue*"
								QueryBuilders.wildcardQuery(key, wildcard))
						.setFrom(from).setSize(size)
						// .setExplain(true)
						.execute().actionGet();

				jsonMap = getHitsMap(myresponse.getHits());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return jsonMap;
	}

	public static Map<String, String> selectWildcardQueryTop100(String index_name, String index_type, String key,
			String wildcard) {
		Map<String, String> jsonMap = null;

		try {

			jsonMap = selectWildcardQuery(index_name, index_type, key, wildcard, 0, 100);

		} catch (Exception e) {
			e.printStackTrace();
		}
		return jsonMap;
	}

	public static Map<String, String> selectRangeQuery(String index_name, String index_type, String key, int gt, int lt,
			int from, int size) {
		Map<String, String> jsonMap = null;

		try {
			if (index_name != null && !"".equals(index_name) && client != null) {
				index_name = index_name.trim().toLowerCase();// 转换成小写
				if (from < 0) {
					from = 0;
				}

				if (size < 0) {
					size = 100;
				}
				if ((from + size) > MAX_RESULT) {
					size = MAX_RESULT - from;
				}

				SearchRequestBuilder responsebuilder = client.prepareSearch(index_name).setTypes(index_type);

				SearchResponse myresponse = responsebuilder
						.setQuery(
								/**
								 * range过滤允许我们按照指定范围查找一批数据
								 * 
								 * 范围操作符包含：
								 * 
								 * gt :: 大于 gte:: 大于等于 lt :: 小于 lte:: 小于等于
								 * 
								 */
								// 12.rang查询
								QueryBuilders.rangeQuery(key).gt(gt).lt(lt))
						.setFrom(from).setSize(size).addSort("id", SortOrder.ASC)
						// .setExplain(true)
						.execute().actionGet();

				jsonMap = getHitsMap(myresponse.getHits());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return jsonMap;
	}

	public static Map<String, String> selectRangeQueryTop100(String index_name, String index_type, String key, int gt,
			int lt) {
		Map<String, String> jsonMap = null;

		try {
			jsonMap = selectRangeQuery(index_name, index_type, key, gt, lt, 0, 100);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return jsonMap;
	}

	/**
	 * 正则表达式查询
	 * 
	 * @param index_name
	 * @param index_type
	 * @param key
	 * @param regexp
	 * @return
	 */
	public static Map<String, String> selectRegexpQuery(String index_name, String index_type, String key, String regexp,
			int from, int size) {
		Map<String, String> jsonMap = null;

		try {
			if (index_name != null && !"".equals(index_name) && client != null) {
				index_name = index_name.trim().toLowerCase();// 转换成小写
				if (from < 0) {
					from = 0;
				}

				if (size < 0) {
					size = 100;
				}
				if ((from + size) > MAX_RESULT) {
					size = MAX_RESULT - from;
				}

				SearchRequestBuilder responsebuilder = client.prepareSearch(index_name).setTypes(index_type);

				SearchResponse myresponse = responsebuilder
						.setQuery(

								// 14.正则表达式查询
								QueryBuilders.regexpQuery(key, regexp))
						.setFrom(from).setSize(size)
						// .setExplain(true)
						.execute().actionGet();
				jsonMap = getHitsMap(myresponse.getHits());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return jsonMap;
	}

	public static Map<String, String> selectRegexpQueryTop100(String index_name, String index_type, String key,
			String regexp) {
		Map<String, String> jsonMap = null;

		try {

			jsonMap = selectRegexpQuery(index_name, index_type, key, regexp, 0, 100);

		} catch (Exception e) {
			e.printStackTrace();
		}
		return jsonMap;
	}

	/************************************
	 * select end
	 ******************************************/
	public static Map<String, String> getHitsMap(SearchHits hits) {
		Map<String, String> jsonMap = null;

		if (hits != null && hits.getHits() != null && hits.getHits().length > 0) {
			jsonMap = new HashMap<String, String>();
			for (int i = 0; i < hits.getHits().length; i++) {
				// System.out.println(hits.getHits()[i].getId()+":"+hits.getHits()[i].getSourceAsString());
				jsonMap.put(hits.getHits()[i].getId(), hits.getHits()[i].getSourceAsString());
			}
		}

		return jsonMap;
	}

	/************************************
	 * select page start
	 ******************************************/

	/**
	 * 使用分词查询,并分页
	 *
	 * @param index
	 *            索引名称
	 * @param type
	 *            类型名称,可传入多个type逗号分隔
	 * @param startPage
	 *            当前页
	 * @param pageSize
	 *            每页显示条数
	 * @param query
	 *            查询条件
	 * @param fields
	 *            需要显示的字段，逗号分隔（缺省为全部字段）
	 * @param sortField
	 *            排序字段
	 * @param highlightField
	 *            高亮字段
	 * @return
	 */
	public static PageInfo<Map<String, Object>> searchDataPage(String index_name, String type, int pageNum,
			int pageSize, QueryBuilder query, String fields, String sortField, String sortType, String highlightField) {
		PageInfo<Map<String, Object>> pageInfo = null;
		if (index_name != null && !"".equals(index_name) && client != null) {
			index_name = index_name.trim().toLowerCase();// 转换成小写
			if (pageNum <= 0) {
				pageNum = 1;
			}
			if (pageSize <= 0) {
				pageSize = 10;
			}
			int pages = 0;
			long total = 0;
			int startRow = (pageNum - 1) * pageSize;
			int endRow = pageNum * pageSize;

			SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index_name);
			if (StringUtils.isNotEmpty(type)) {
				searchRequestBuilder.setTypes(type.split(","));
			}
			searchRequestBuilder.setSearchType(SearchType.QUERY_THEN_FETCH);

			// 需要显示的字段，逗号分隔（缺省为全部字段）
			if (StringUtils.isNotEmpty(fields)) {
				searchRequestBuilder.setFetchSource(fields.split(","), null);
			}

			// 排序字段
			if (StringUtils.isNotEmpty(sortField)) {
				if (sortType != null && "ASC".equals(sortType.trim().toUpperCase())) {
					searchRequestBuilder.addSort(sortField, SortOrder.ASC);
				} else {
					searchRequestBuilder.addSort(sortField, SortOrder.DESC);
				}

			}

			// 高亮（xxx=111,aaa=222）
			if (StringUtils.isNotEmpty(highlightField)) {
				HighlightBuilder highlightBuilder = new HighlightBuilder();

				// highlightBuilder.preTags("<span style='color:red' >");//设置前缀
				// highlightBuilder.postTags("</span>");//设置后缀

				// 设置高亮字段
				highlightBuilder.field(highlightField);
				searchRequestBuilder.highlighter(highlightBuilder);
			}

			// searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery());
			searchRequestBuilder.setQuery(query);

			// 分页应用
			searchRequestBuilder.setFrom(startRow).setSize(endRow);

			// 设置是否按查询匹配度排序
			searchRequestBuilder.setExplain(true);

			// 打印的内容 可以在 Elasticsearch head 和 Kibana 上执行查询
			LOGGER.info("\n{}", searchRequestBuilder);

			// 执行搜索,返回搜索响应信息
			SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();

			long totalHits = searchResponse.getHits().totalHits;
			long length = searchResponse.getHits().getHits().length;

			total = totalHits;

			if (total % pageSize == 0) {
				pages = (int) (total / pageSize);
			} else {
				pages = (int) total / pageSize + 1;
			}
			LOGGER.debug("共查询到[{}]条数据,处理数据条数[{}]", totalHits, length);

			if (searchResponse.status().getStatus() == 200) {
				// 解析对象
				List<Map<String, Object>> sourceList = setSearchResponse(searchResponse, highlightField);

				pageInfo = new PageInfo<Map<String, Object>>(pageNum, pageSize, startRow, endRow, total, pages,
						sortField, sortType, sourceList);
			}
		}
		return pageInfo;

	}

	/**
	 * 使用分词查询
	 *
	 * @param index
	 *            索引名称
	 * @param type
	 *            类型名称,可传入多个type逗号分隔
	 * @param query
	 *            查询条件
	 * @param size
	 *            文档大小限制
	 * @param fields
	 *            需要显示的字段，逗号分隔（缺省为全部字段）
	 * @param sortField
	 *            排序字段
	 * @param highlightField
	 *            高亮字段
	 * @return
	 */
	public static List<Map<String, Object>> searchListData(String index_name, String type, QueryBuilder query,
			Integer size, String fields, String sortField, String highlightField) {
		if (index_name != null && !"".equals(index_name) && client != null) {
			index_name = index_name.trim().toLowerCase();// 转换成小写

			SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index_name);
			if (StringUtils.isNotEmpty(type)) {
				searchRequestBuilder.setTypes(type.split(","));
			}

			if (StringUtils.isNotEmpty(highlightField)) {
				HighlightBuilder highlightBuilder = new HighlightBuilder();
				// 设置高亮字段
				highlightBuilder.field(highlightField);
				searchRequestBuilder.highlighter(highlightBuilder);
			}

			searchRequestBuilder.setQuery(query);

			if (StringUtils.isNotEmpty(fields)) {
				searchRequestBuilder.setFetchSource(fields.split(","), null);
			}
			searchRequestBuilder.setFetchSource(true);

			if (StringUtils.isNotEmpty(sortField)) {
				searchRequestBuilder.addSort(sortField, SortOrder.DESC);
			}

			if (size != null && size > 0) {
				searchRequestBuilder.setSize(size);
			}

			// 打印的内容 可以在 Elasticsearch head 和 Kibana 上执行查询
			LOGGER.info("\n{}", searchRequestBuilder);

			SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();

			long totalHits = searchResponse.getHits().totalHits;
			long length = searchResponse.getHits().getHits().length;

			LOGGER.info("共查询到[{}]条数据,处理数据条数[{}]", totalHits, length);

			if (searchResponse.status().getStatus() == 200) {
				// 解析对象
				return setSearchResponse(searchResponse, highlightField);
			}

		}
		return null;

	}

	/**
	 * 高亮结果集 特殊处理
	 *
	 * @param searchResponse
	 * @param highlightField
	 */
	private static List<Map<String, Object>> setSearchResponse(SearchResponse searchResponse, String highlightField) {

		List<Map<String, Object>> sourceList = new ArrayList<Map<String, Object>>();
		StringBuffer stringBuffer = new StringBuffer();

		for (SearchHit searchHit : searchResponse.getHits().getHits()) {
			searchHit.getSourceAsMap().put("id", searchHit.getId());

			if (StringUtils.isNotEmpty(highlightField)) {

				System.out.println("遍历 高亮结果集，覆盖 正常结果集" + searchHit.getSourceAsMap());
				Text[] text = searchHit.getHighlightFields().get(highlightField).getFragments();

				if (text != null) {
					for (Text str : text) {
						stringBuffer.append(str.string());
					}
					// 遍历 高亮结果集，覆盖 正常结果集
					searchHit.getSourceAsMap().put(highlightField, stringBuffer.toString());
				}
			}
			sourceList.add(searchHit.getSourceAsMap());
		}

		return sourceList;
	}

	

	/************************************
	 * select page end
	 ******************************************/

}