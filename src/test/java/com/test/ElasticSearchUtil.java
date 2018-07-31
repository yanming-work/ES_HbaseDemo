package com.test;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;


/**
 * 
 * @类 名： ElasticSearchUtil 
 * @功能描述： ES工具类 
 * @作者信息： 严明
 * @创建时间：2018年6月14日下午2:31:50 
 * @修改备注：
 * * //////////////////////////////////////////////////////////////////// 
 * //                          _ooOoo_                               // 
 * //                         o8888888o                              // 
 * //                         88" . "88                              // 
 * //                         (| ^_^ |)                              // 
 * //                         O\  =  /O                              // 
 * //                      ____/`---'\____                           // 
 * //                    .'  \\|     |//  `.                         // 
 * //                   /  \\|||  :  |||//  \                        // 
 * //                  /  _||||| -:- |||||-  \                       // 
 * //                  |   | \\\  -  /// |   |                       // 
 * //                  | \_|  ''\---/''  |   |                       // 
 * //                  \  .-\__  `-`  ___/-. /                       // 
 * //                ___`. .'  /--.--\  `. . ___                     // 
 * //              ."" '<  `.___\_<|>_/___.'  >'"".                  // 
 * //            | | :  `- \`.;`\ _ /`;.`/ - ` : | |                 // 
 * //            \  \ `-.   \_ __\ /__ _/   .-` /  /                 // 
 * //      ========`-.____`-.___\_____/___.-`____.-'========         // 
 * //                           `=---='                              // 
 * //      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^        // 
 * //                 佛祖保佑                          再无Bug                        // 
 * //////////////////////////////////////////////////////////////////// 
 *   
 */
@SuppressWarnings({ "unchecked", "resource" })
public class ElasticSearchUtil {
	public final static String HOST = PropertiesUtil.getValueByKey("config.properties","es.host");
	// http请求的端口是9200，客户端是9300
	public final static int PORT = Integer.valueOf(PropertiesUtil.getValueByKey("config.properties","es.port"));
	public final static String CLUSTER_NAME = PropertiesUtil.getValueByKey("config.properties","es.cluster_name");
	//默认值是10000，加大的话需要修改index.max_result_window参数来增大结果窗口大小。
	public final static int MAX_RESULT = Integer.valueOf(PropertiesUtil.getValueByKey("config.properties","es.max_result_window"));
	
	
	
	private static TransportClient client = null;

	private static IndicesAdminClient adminClient = null;

	
	static{
		// 设置集群名称
				try {
					Settings settings = Settings.builder().put("cluster.name", CLUSTER_NAME).build();
					// 创建client
					
					//client = new PreBuiltTransportClient(settings).addTransportAddresses(new TransportAddress(InetAddress.getByName(HOST), PORT));
					
					client = new PreBuiltTransportClient(settings).addTransportAddresses(new InetSocketTransportAddress(InetAddress.getByName(HOST), PORT));
					
					if(client!=null){
						adminClient = client.admin().indices();	
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
	}
	/**
	 * 获取ES连接
	 * 
	 * @Title : getConnection
	 * @功能描述:  @设定文件：
	 * @返回类型：void
	 * @throws ：
	 */
	public static void getConnection() {
		
		try {
			// 设置集群名称
			Settings settings = Settings.builder().put("cluster.name", CLUSTER_NAME).build();
			// 创建client
			if(client==null){
				
				client = new PreBuiltTransportClient(settings)
						.addTransportAddresses(new InetSocketTransportAddress(InetAddress.getByName(HOST), PORT));
				
				if(client!=null){
					adminClient = client.admin().indices();	
				}
			}else{
				closeConnection();
				
				client = new PreBuiltTransportClient(settings)
						.addTransportAddresses(new InetSocketTransportAddress(InetAddress.getByName(HOST), PORT));
				
				if(client!=null){
					adminClient = client.admin().indices();	
				}
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 关闭ES连接
	 * 
	 * @Title : closeConnection
	 * @功能描述:  @设定文件：
	 * @返回类型：void
	 * @throws ：
	 */
	public static void closeConnection() {
		if (adminClient != null) {
			adminClient = null;
		}

		if (client != null) {
			client.close();
			client = null;
		}
	}
	
	
	
	 /**
     * 查看集群信息
     */
    public static List<DiscoveryNode>  info() {
    	 List<DiscoveryNode> nodes =null;
    	try {
	    	if (client == null) {
				getConnection();// 获取连接
			}
	
	        nodes = client.connectedNodes();
	        for (DiscoveryNode node : nodes) {
	            System.out.println(node.getHostAddress());
	        }
    	} catch (Exception e) {
			e.printStackTrace();
		} 
    	return nodes;
    }

	/************************************
	 * index start
	 ******************************************/

	/**
	 * 判断是否存在该索引
	 * 
	 * @param index_name
	 *            索引名称
	 * @return
	 */
	private static boolean isIndexExists(String index_name) {
		boolean flag = false;
		try {
			if (adminClient == null) {
				getConnection();// 获取连接
			}

			IndicesExistsRequestBuilder builder = adminClient.prepareExists(index_name);
			IndicesExistsResponse res = builder.get();
			flag = res.isExists();
		} catch (Exception e) {
			e.printStackTrace();
		} 
		return flag;

	}

	/**
	 * 5.*之后，把string字段设置为了过时字段，引入text，keyword字段
	 * 
	 * keyword：存储数据时候，不会分词建立索引
	 * text：存储数据时候，会自动分词，并生成索引（这是很智能的，但在有些字段里面是没用的，所以对于有些字段使用text则浪费了空间）。
	 *
	 * 如果在添加分词器的字段上，把type设置为keyword，则创建索引会失败
	 */
	@SuppressWarnings("unused")
	private static XContentBuilder getIndexSourceTest() throws IOException {
		XContentBuilder source = XContentFactory.jsonBuilder().startObject().startObject("test_type2")
				.startObject("properties")
				// code字段
				.startObject("code").field("type", "text").field("index", true).field("fielddata", true).endObject()
				// 名称字段
				.startObject("name").field("type", "keyword").field("store", false).field("index", true).endObject()
				// 信息字段
				.startObject("info").field("type", "keyword").field("store", false).field("index", true).endObject()
				// 主要内容字段
				.startObject("content").field("type", "text").field("store", true).field("index", true).endObject()
				.startObject("my_title").field("type", "keyword").field("store", true).field("index", true).endObject()
				.startObject("you_title").field("type", "keyword").field("store", true).field("index", true).endObject()
				.startObject("isDelete").field("type", "boolean").field("store", true).field("index", true).endObject()
				.startObject("age").field("type", "long").field("store", true).field("index", true).endObject()

				.endObject().endObject().endObject();
		return source;
	}

	private static XContentBuilder getIndexSource(String index_type, List<EsIndexSource> esIndexSourceList)
			throws IOException {

		XContentBuilder source = XContentFactory.jsonBuilder().startObject().startObject(index_type);
		if (esIndexSourceList != null && esIndexSourceList.size() > 0) {
			source = source.startObject("properties");
			for (EsIndexSource esIndexSource : esIndexSourceList) {
				source = source.startObject(esIndexSource.getName()).field("type", esIndexSource.getType())
						.field("index", esIndexSource.isIndex()).field("store", esIndexSource.isStore()).endObject();
			}
			source = source.endObject();
		}
		source = source.endObject().endObject();
		return source;

		/***
		 * XContentBuilder source =
		 * XContentFactory.jsonBuilder().startObject().startObject("test_type2")
		 * .startObject("properties") // code字段
		 * .startObject("code").field("type", "text").field("index",
		 * true).field("fielddata", true).endObject() // 名称字段
		 * .startObject("name").field("type", "keyword").field("store",
		 * false).field("index", true).endObject() // 信息字段
		 * .startObject("info").field("type", "keyword").field("store",
		 * false).field("index", true).endObject() // 主要内容字段
		 * .startObject("content").field("type", "text").field("store",
		 * true).field("index", true).endObject()
		 * .startObject("my_title").field("type", "keyword").field("store",
		 * true).field("index", true).endObject()
		 * .startObject("you_title").field("type", "keyword").field("store",
		 * true).field("index", true).endObject()
		 * .startObject("isDelete").field("type", "boolean").field("store",
		 * true).field("index", true).endObject()
		 * .startObject("age").field("type", "long").field("store",
		 * true).field("index", true).endObject()
		 * 
		 * .endObject().endObject().endObject();
		 */

	}

	
	

/**
	 * 判断指定的索引名是否存在
	 * 
	 * @param index_name
	 *            索引名
	 * @return 存在：true; 不存在：false;
	 */
	public  boolean isExistsIndex(String index_name) {
		boolean flag = false;
		try {
			if (adminClient == null) {
				getConnection();// 获取连接
			}
			IndicesExistsResponse response = adminClient
					.exists(new IndicesExistsRequest()
							.indices(new String[] { index_name })).actionGet();
			flag= response.isExists();
		} catch (Exception e) {
			System.out.println("创建索引失败！" + e);
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
	public  boolean isExistsType(String index_name, String index_type) {
		boolean flag = false;
		try {
			if (adminClient == null) {
				getConnection();// 获取连接
			}
			TypesExistsResponse response = adminClient
					.typesExists(
							new TypesExistsRequest(new String[] { index_name },
									index_type)).actionGet();
			flag= response.isExists();
		} catch (Exception e) {
			System.out.println("创建索引失败！" + e);
		}
		return flag;
	}
	
	
	/**
	 * 创建索引
	 */
	public static boolean createIndex(String index_name, String index_type) {
		boolean flag = false;
		if (index_name != null && !"".equals(index_name) && index_type != null && !"".equals(index_type)) {
			try {
				if (isIndexExists(index_name)) {
					System.out.println("索引对象已经存在，无法创建！");
					return false;
				}
				if (adminClient == null) {
					getConnection();// 获取连接
				}
				CreateIndexRequestBuilder builder = adminClient.prepareCreate(index_name);
				// 直接创建Map结构的setting
				Map<String, Object> settings = new HashMap<String, Object>();
				settings.put("number_of_shards", 5); // 分片数
				settings.put("number_of_replicas", 1); // 副本数
				settings.put("refresh_interval", "10s"); // 刷新间隔
				builder.setSettings(settings);

				builder.addMapping(index_type, getIndexSource(index_type, null));

				CreateIndexResponse response = builder.get();
				System.out.println(response.isAcknowledged() ? "索引创建成功！" : "索引创建失败！");
				flag = response.isAcknowledged();
		       
			} catch (Exception e) {
				System.out.println("创建索引失败！" + e);
			} 

		}

		return flag;
	}
	
	

	/**
	 * public static boolean createIndex(String index_name ){ boolean
	 * flag=false; if(index_name!=null && !"".equals(index_name)){ try{
	 * if(client==null){ getConnection();//获取连接 }
	 * 
	 * CreateIndexRequestBuilder builder =
	 * client.admin().indices().prepareCreate(index_name);
	 * 
	 * CreateIndexResponse res = builder.get();
	 * System.out.println(res.isAcknowledged() ?"索引创建成功！" : "索引创建失败！");
	 * flag=res.isAcknowledged(); } catch (Exception e) {
	 * System.out.println("创建索引失败！"+ e); }finally{ closeConnection(); }
	 * 
	 * }
	 * 
	 * return flag;
	 * 
	 * }
	 **/

	/**
	 * 删除索引
	 * 
	 * @param index_name
	 *            索引名
	 * @return
	 */
	public static boolean deleteIndex(String index_name) {
		boolean flag = false;
		try {
			if (adminClient == null) {
				getConnection();// 获取连接
			}
			DeleteIndexResponse response = adminClient.prepareDelete(index_name).execute().actionGet();
			flag = response.isAcknowledged();

		} catch (Exception e) {
			e.printStackTrace();
		} 
		return flag;
	}

	/**
	 * 关闭索引
	 * 
	 * @param index
	 * @return
	 */
	public static boolean closeIndex(String index) {
		boolean flag = false;
		try {
			if (adminClient == null) {
				getConnection();// 获取连接
			}
			CloseIndexResponse response = adminClient.prepareClose(index).get();
			flag = response.isAcknowledged();

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
	public static boolean openIndex(String index) {
		boolean flag = false;
		try {
			if (adminClient == null) {
				getConnection();// 获取连接
			}
			OpenIndexResponse response = adminClient.prepareOpen(index).get();
			flag = response.isAcknowledged();

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
	public void reindex(String index,int size) {
		try {
			if (client == null) {
				getConnection();// 获取连接
			}
			if(size<=0){
				size=100;
			}	
         	if(size>MAX_RESULT){
     			size=MAX_RESULT;
     		}
         	
			SearchResponse scrollResp = client.prepareSearch(index)//
					.setScroll(new TimeValue(60000))//
					.setQuery(QueryBuilders.matchAllQuery())//
					.setSize(size).get(); // max of 100 hits will be returned for
			// Scroll until no hits are returned
			do {
				for (SearchHit hit : scrollResp.getHits().getHits()) {
					client.prepareIndex(index, hit.getType(), hit.getId()).setSource(hit.getSourceAsString()).execute().actionGet();
				}
				scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
			} while (scrollResp.getHits().getHits().length != 0);
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
	public static void indexStats(String index) {
		try {
			if (adminClient == null) {
				getConnection();// 获取连接
			}
			IndicesStatsResponse response = adminClient.prepareStats(index).all().get();
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
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}

	/************************************
	 * index end
	 ******************************************/

	/************************************
	 * save start
	 ******************************************/
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
			if (client == null) {
				getConnection();// 获取连接
			}
			if (id != null && !"".equals(id)) {
				client.prepareIndex(index_name, index_type).setId(id).setSource(sourceMap).get();
				flag = true;
			} else {
				flag = saveMap(index_name, index_type, sourceMap);
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
			if (client == null) {
				getConnection();// 获取连接
			}
			client.prepareIndex(index_name, index_type).setSource(sourceMap).get();
			flag = true;

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
			if (client == null) {
				getConnection();// 获取连接
			}
			IndexResponse response = client.prepareIndex(index_name, index_type).setSource(sourceMap).get();
			if (response != null) {
				id = response.getId();
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return id;
	}

	/**
	 * 保存 jsonStr
	 * 
	 * @Title : insertJsonStr
	 * @功能描述: 
	 * @设定文件：@param index_name
	 * @设定文件：@param index_type
	 * @设定文件：@param id
	 * @设定文件：@param jsonStr
	 * @设定文件：@return
	 * @返回类型：long
	 * @throws ：
	 */
	public static boolean insertJsonStr(String index_name, String index_type, String id, String jsonStr) {
		boolean flag = false;
		if (index_name != null && !"".equals(index_name) && index_type != null && !"".equals(index_type)) {
			try {
				if (client == null) {
					getConnection();// 获取连接
				}
				if (id != null && !"".equals(id)) {
					client.prepareIndex(index_name, index_type, id).setSource(jsonStr, XContentType.JSON).get();
					flag = true;
					/**
					 *
					 * IndexResponse response =client.prepareIndex(index_name,
					 * index_type,id).setSource(jsonStr,
					 * XContentType.JSON).get(); System.out.println("索引名称：" +
					 * response.getIndex()); System.out.println("类型：" +
					 * response.getType()); System.out.println("文档ID：" +
					 * response.getId()); // 第一次使用是1
					 * System.out.println("当前实例状态：" + response.status());
					 */

				} else {
					flag = insertJsonStr(index_name, index_type, jsonStr);

				}

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return flag;
	}
	
	
	public static boolean insertJsonStrMapList(String index_name, String index_type,Map<String,String> mapjson ) {
		boolean flag = false;
		if (index_name != null && !"".equals(index_name) && index_type != null && !"".equals(index_type)) {
			try {
				if(mapjson!=null ){
					if (client == null) {
						getConnection();// 获取连接
					}
					 for (Map.Entry<String,String> entry : mapjson.entrySet()) { 
						 String id=entry.getKey();
						 String jsonStr=entry.getValue();
							if (id != null && !"".equals(id)) {
								client.prepareIndex(index_name, index_type, id).setSource(jsonStr, XContentType.JSON).get();
								flag = true;
								/**
								 *
								 * IndexResponse response =client.prepareIndex(index_name,
								 * index_type,id).setSource(jsonStr,
								 * XContentType.JSON).get(); System.out.println("索引名称：" +
								 * response.getIndex()); System.out.println("类型：" +
								 * response.getType()); System.out.println("文档ID：" +
								 * response.getId()); // 第一次使用是1
								 * System.out.println("当前实例状态：" + response.status());
								 */

							} else {
								flag = insertJsonStr(index_name, index_type, jsonStr);

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
	 * 保存 jsonStr
	 * 
	 * @Title : insertJsonStr
	 * @功能描述: 
	 * @设定文件：@param index_name
	 * @设定文件：@param index_type
	 * @设定文件：@param jsonStr
	 * @设定文件：@return
	 * @返回类型：long
	 * @throws ：
	 */
	public static boolean insertJsonStr(String index_name, String index_type, String jsonStr) {
		boolean flag = false;
		if (index_name != null && !"".equals(index_name) && index_type != null && !"".equals(index_type)) {
			try {
				if (client == null) {
					getConnection();// 获取连接
				}

				client.prepareIndex(index_name, index_type).setSource(jsonStr, XContentType.JSON).get();

				/**
				 *
				 * IndexResponse response =client.prepareIndex(index_name,
				 * index_type,id).setSource(jsonStr, XContentType.JSON).get();
				 * System.out.println("索引名称：" + response.getIndex());
				 * System.out.println("类型：" + response.getType());
				 * System.out.println("文档ID：" + response.getId()); // 第一次使用是1
				 * System.out.println("当前实例状态：" + response.status());
				 */
				flag = true;

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
				if (client == null) {
					getConnection();// 获取连接
				}
				if (id != null && !"".equals(id)) {
					client.prepareIndex(index_name, index_type, id)
							.setSource(jsonObject.toJSONString(), XContentType.JSON).get();
					flag = true;
				} else {
					flag = insertJSONObject(index_name, index_type, jsonObject);
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
				if (client == null) {
					getConnection();// 获取连接
				}

				client.prepareIndex(index_name, index_type).setSource(jsonObject.toJSONString(), XContentType.JSON)
						.get();
				flag = true;

			} catch (Exception e) {
				e.printStackTrace();
			} 
		}
		return flag;
	}
	
	/**
	 * 保存Obj
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
	public static boolean insertObject(String index_name, String index_type, String id,Object object) {
		boolean flag = false;
		if (index_name != null && !"".equals(index_name) && index_type != null && !"".equals(index_type) && object!=null) {
			try {
				if (client == null) {
					getConnection();// 获取连接
				}
				  String  jsonStr = JSONObject.toJSONString(object);
				if (id != null && !"".equals(id)) {
					flag=insertJsonStr(index_name, index_type, id, jsonStr);
				} else {
					flag = insertJsonStr(index_name, index_type, jsonStr);
				}

			} catch (Exception e) {
				e.printStackTrace();
			} 
		}
		return flag;
	}
	
	
	/**
	 * 保存Obj
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
		if (index_name != null && !"".equals(index_name) && index_type != null && !"".equals(index_type) && object!=null) {
			try {
				if (client == null) {
					getConnection();// 获取连接
				}
				String  jsonStr = JSONObject.toJSONString(object);
				
				flag = insertJsonStr(index_name, index_type, jsonStr);

			} catch (Exception e) {
				e.printStackTrace();
			} 
		}
		return flag;
	}
	

	public static boolean insertList(String index_name, String index_type, List<?> list) {
		boolean flag = false;
		if (index_name != null && !"".equals(index_name) && index_type != null && !"".equals(index_type) && list!=null && list.size()>0) {
			try {
				if (client == null) {
					getConnection();// 获取连接
				}
				
				for (Object object : list) {
					String  jsonStr = JSONObject.toJSONString(object);
					flag = flag || insertJsonStr(index_name, index_type, jsonStr);
				}

			} catch (Exception e) {
				e.printStackTrace();
			} 
		}
		return flag;
	}
	
	/**
	 * 添加文档
	   * @Title : addDoc 
	   * @功能描述: 
	   * @设定文件：@param index
	   * @设定文件：@param type
	   * @设定文件：@param id
	   * @设定文件：@param object
	   * @设定文件：@return 
	   * @返回类型：boolean 
	   * @throws ：
	 */
	public boolean addDoc(String index,String type, Object id, Object object) {
		boolean flag = false;
		try {
			if (client == null) {
				getConnection();// 获取连接
			}
			client.prepareIndex(index, type, id.toString()).setSource(JSON.toJSONString(object)).get();
			flag=true;
		} catch (Exception e) {
			e.printStackTrace();
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
     * 修改 jsonStr
       * @Title : updateJsonStr 
       * @功能描述: 
       * @设定文件：@param index_name
       * @设定文件：@param index_type
       * @设定文件：@param id
       * @设定文件：@param jsonStr
       * @设定文件：@return 
       * @返回类型：long 
       * @throws ：
     */
    public static boolean updateJsonStr(String index_name,String index_type,String id,String jsonStr){
    	boolean flag = false;
    	if(index_name!=null && !"".equals(index_name) && index_type!=null && !"".equals(index_type) && id!=null &&!"".equals(id)){
	    	try{
	    		if(client==null){
    				getConnection();//获取连接
    			}
	    		// UpdateResponse response = 
	    		client.prepareUpdate(index_name, index_type,id).setDoc(jsonStr, XContentType.JSON).get();
	    		flag = true;
		          
		    	/**
		    	 *UpdateResponse response = client.prepareUpdate(index_name, index_type,id).setDoc(jsonStr, XContentType.JSON).get();
	    		System.out.println("索引名称：" + response.getIndex());
		        System.out.println("类型：" + response.getType());
		        System.out.println("文档ID：" + response.getId()); // 第一次使用是1
		        System.out.println("当前实例状态：" + response.status());
		        //多次index这个版本号会变  
		        System.out.println("response.version:"+responseVersion);
		    	 */
	        
	    	}catch (Exception e) {
	        	e.printStackTrace();
			}
    	}
    	return flag;
    }
    
    
    /**
     * 修改JSONObject
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
    public static boolean updateJSONObject(String index_name,String index_type,String id,JSONObject jsonObject){
    	boolean flag = false;
    	if(index_name!=null && !"".equals(index_name) && index_type!=null && !"".equals(index_type) && id!=null &&!"".equals(id)){
	    	try{
	    		if(client==null){
    				getConnection();//获取连接
    			}
	    		client.prepareUpdate(index_name, index_type,id).setDoc(jsonObject, XContentType.JSON).get().getVersion();
	    		flag = true;
	        
	    	}catch (Exception e) {
	        	e.printStackTrace();
			}
    	}
    	return flag;
    }
    
    
    /**
     * 修改Object
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
    public static boolean updateObject(String index_name,String index_type,String id,Object object){
    	boolean flag = false;
    	if(index_name!=null && !"".equals(index_name) && index_type!=null && !"".equals(index_type) && id!=null &&!"".equals(id)){
	    	try{
	    		if(client==null){
    				getConnection();//获取连接
    			}
	    		
	    		String  jsonStr = JSONObject.toJSONString(object);
	    		flag =updateJsonStr(index_name, index_type, id, jsonStr);
	    			   
		    	
	    	}catch (Exception e) {
	        	e.printStackTrace();
			}
    	}
    	return flag;
    }
	
    
    
    
    /**
	 * 
	 * @Description:更新文档
	 */
	public boolean updateDoc(String index,String type, Object id, Object object) {
		boolean flag = false;
		try{
    		if(client==null){
				getConnection();//获取连接
			}
    		client.prepareUpdate(index, type, id.toString()).setDoc(JSON.toJSONString(object)).get();
    		flag = true;
		}catch (Exception e) {
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
     * 删除
       * @Title : deleteById 
       * @功能描述: 
       * @设定文件：@param index_name
       * @设定文件：@param index_type
       * @设定文件：@param id 
       * @返回类型：void 
       * @throws ：
     */
    public static boolean deleteById(String index_name,String index_type,String id){
    	boolean flag = false;
    	if(index_name!=null && !"".equals(index_name) && index_type!=null && !"".equals(index_type) && id!=null &&!"".equals(id)){
	    	try{
	    		if(client==null){
    				getConnection();//获取连接
    			}
	    		client.prepareDelete(index_name, index_type,id).get();
	    		flag =true;
	    		/**
	    		 * DeleteResponse response = client.prepareDelete(index_name, index_type,id).get();
		        System.out.println("索引名称：" + response.getIndex());
		        System.out.println("类型：" + response.getType());
		        System.out.println("文档ID：" + response.getId());
		        System.out.println("当前实例状态：" + response.status());
		      //多次index这个版本号会变  
		        System.out.println("response.version:"+response.getVersion());
		        */
	    		
	    	}catch (Exception e) {
	        	e.printStackTrace();
			}
    	}
    	return flag;
    }
    
    /**
	 * 
	 * @Description:删除文档
	 */
	public boolean delDoc(String index,String type, Object id) {
		
		boolean flag = false;
		try{
    		if(client==null){
				getConnection();//获取连接
			}
    		client.prepareDelete(index, type, id.toString()).get();
    		flag = true;
		}catch (Exception e) {
        	e.printStackTrace();
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
     * 根据ID查询
       * @Title : selectById 
       * @功能描述: 
       * @设定文件：@param index_name
       * @设定文件：@param index_type
       * @设定文件：@param id
       * @设定文件：@return 
       * @返回类型：String 
       * @throws ：
     */
    public static String selectById(String index_name,String index_type,String id) {
    	String resultStr=null;
        try {
        	if(client==null){
				getConnection();//获取连接
			}
            GetResponse response = client.prepareGet(index_name, index_type,id).execute().actionGet();
            resultStr=response.getSourceAsString();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultStr;
    }
    
    public static Map<String,String>  selectByKeyValue(String index_name,String index_type,String key,String value,int from,int size) {
    	Map<String,String> jsonMap= null;
    	 try {
         	if(client==null){
 				getConnection();//获取连接
 			}
         	
         	if(from<0){
     			from=0;
     		}
     		
     		if(size<0){
     			size=10;
     		}
     		
         	if((from+size)>MAX_RESULT){
     			size=MAX_RESULT-from;
     		}
         	
     		
     		
         		SearchRequestBuilder responsebuilder = client.prepareSearch(index_name).setTypes(index_type) ;
         		SearchResponse myresponse=responsebuilder.setQuery(                
                 //基本查询
                 QueryBuilders.matchPhraseQuery(key, value)                
                 )  
                 .setFrom(from).setSize(size)
 //                .setExplain(true)
                 .execute().actionGet(); 
    	
         		jsonMap=getHitsMap(myresponse.getHits());
         		 
    	 } catch (Exception e) {
             e.printStackTrace();
         }
    	 return jsonMap;
    }
    
    
    
    public static Map<String,String>  selectByKeyValueTop10(String index_name,String index_type,String key,String value) {
    	Map<String,String> jsonMap= null;
    	 try {
         	if(client==null){
 				getConnection();//获取连接
 			}
         	jsonMap=selectByKeyValue(index_name, index_type, key, value, 0, 10);
         		 
    	 } catch (Exception e) {
             e.printStackTrace();
         }
    	 return jsonMap;
    }
    
    
    public static Map<String,String> selectByKeyValuesTop10(String index_name,String index_type,String key,String ... values) {
    	Map<String,String> jsonMap= null;
    	 try {
    		 jsonMap= selectByKeyValues(index_name, index_type, key, 0, 10, values);

    	 } catch (Exception e) {
             e.printStackTrace();
         }
    	 return jsonMap;
    }
    
    
    
    
    public static Map<String,String> selectByKeyValues(String index_name,String index_type,String key,int from,int size,String ... values) {
    	Map<String,String> jsonMap= null;
    	 try {
         	if(client==null){
 				getConnection();//获取连接
 			}
         		SearchRequestBuilder responsebuilder = client.prepareSearch(index_name).setTypes(index_type) ;
         		if(from<0){
         			from=0;
         		}
         		
         		if(size<0){
         			size=10;
         		}
         		if((from+size)>MAX_RESULT){
         			size=MAX_RESULT-from;
         		}
    SearchResponse myresponse=responsebuilder.setQuery(                
            //多词条查询 ?
            //term主要用于精确匹配哪些值，比如数字，日期，布尔值或 not_analyzed 的字符串(未经分析的文本数据类型)： 
            QueryBuilders.termsQuery(key,values)            
            )  
            .setFrom(from).setSize(size)
//            .setExplain(true)
            .execute().actionGet();
    

			jsonMap=getHitsMap(myresponse.getHits());
    	 } catch (Exception e) {
             e.printStackTrace();
         }
    	 return jsonMap;
    }
    
    public static Map<String,String> selectAll(String index_name,String index_type,int from,int size) {
    	Map<String,String> jsonMap= null;
    	 try {
         	if(client==null){
 				getConnection();//获取连接
 			}
         		SearchRequestBuilder responsebuilder = client.prepareSearch(index_name).setTypes(index_type) ;
         		if(from<0){
         			from=0;
         		}
         		
         		if(size<0){
         			size=10;
         		}
         		if((from+size)>MAX_RESULT){
         			size=MAX_RESULT-from;
         		}
    SearchResponse myresponse=responsebuilder.setQuery(                
            //3.查询全部
            QueryBuilders.matchAllQuery()          
            )  
            .setFrom(from).setSize(size)
//            .setExplain(true)
            .execute().actionGet();

    		jsonMap=getHitsMap(myresponse.getHits());
	 } catch (Exception e) {
         e.printStackTrace();
     }
	 return jsonMap;
}
    
    
    
    
    public static Map<String,String> selectAllTop100(String index_name,String index_type) {
    	Map<String,String> jsonMap= null;
    	 try {
         	if(client==null){
 				getConnection();//获取连接
 			}

    		jsonMap=selectAll(index_name, index_type, 0, 100);
	 } catch (Exception e) {
         e.printStackTrace();
     }
	 return jsonMap;
}
    
    
    
    public static Map<String,String> selectCommonTerms(String index_name,String index_type,String key,String value,int from,int size) {
    	Map<String,String> jsonMap= null;
    	
    	 try {
         	if(client==null){
 				getConnection();//获取连接
 			}
         	if(from <0){
         		from=0;
         	}
         	
         	if(size <0){
         		size=100;
         	}	
         	if((from+size)>MAX_RESULT){
     			size=MAX_RESULT-from;
     		}
         	
         		SearchRequestBuilder responsebuilder = client.prepareSearch(index_name).setTypes(index_type) ;
         		SearchResponse myresponse=responsebuilder.setQuery(                
                        //4.常用词查询
                        QueryBuilders.commonTermsQuery(key, value)        
                        )  
                        .setFrom(from).setSize(size)
//                        .setExplain(true)
                        .execute().actionGet();
    	
         		jsonMap=getHitsMap(myresponse.getHits());
         		 
    	 } catch (Exception e) {
             e.printStackTrace();
         }
    	 return jsonMap;
    }

    
    
    public static Map<String,String> selectCommonTermsTop100(String index_name,String index_type,String key,String value) {
    	Map<String,String> jsonMap= null;
    	
    	 try {
         	
         		jsonMap=selectCommonTerms(index_name, index_type, key, value, 0, 100);
         		 
    	 } catch (Exception e) {
             e.printStackTrace();
         }
    	 return jsonMap;
    }
    
    /**
     * multiMatchQuery(Object text, String... fieldNames):text为文本值，fieldNames为字段名称。
		举例说明：name、address为字段名称，13为文本值。查询name字段或者address字段文本值为13的结果集。
     * @return
     */
    public static Map<String,String> selectByMultiMatchQuery(String index_name,String index_type,String text,int from,int size,String ...  fields) {
    	Map<String,String> jsonMap= null;
    	
    	 try {
         	if(client==null){
 				getConnection();//获取连接
 			}
         	if(from <0){
         		from=0;
         	}
         	
         	if(size <0){
         		size=100;
         	}
         	if((from+size)>MAX_RESULT){
     			size=MAX_RESULT-from;
     		}
         		SearchRequestBuilder responsebuilder = client.prepareSearch(index_name).setTypes(index_type) ;
         		SearchResponse myresponse=responsebuilder.setQuery(                
                        //multiMatchQuery(text,fields)其中的fields是字段的名字，可以写好几个，每一个中间用逗号分隔 
                        QueryBuilders.multiMatchQuery(text,fields)    
                        )  
                        .setFrom(from).setSize(size)
//                        .setExplain(true)
                        .execute().actionGet();
    	
         		jsonMap=getHitsMap(myresponse.getHits());
         		 
    	 } catch (Exception e) {
             e.printStackTrace();
         }
    	 return jsonMap;
    }
    
    
    
    public static Map<String,String> selectByMultiMatchQueryTop100(String index_name,String index_type,String text,String ...  fields) {
    	Map<String,String> jsonMap= null;
    	
    	 try {
         		jsonMap=selectByMultiMatchQuery(index_name, index_type, text, 0, 100, fields);
         		 
    	 } catch (Exception e) {
             e.printStackTrace();
         }
    	 return jsonMap;
    }
    
    /**
     * 查询任意字段文本值为?的结果集。multi_match为指定某几个字段，query_string是查所有的字段。
     * @return
     */
    public static Map<String,String> selectQuery(String index_name,String index_type,String text,int from,int size) {
    	Map<String,String> jsonMap= null;
    	
    	 try {
         	if(client==null){
 				getConnection();//获取连接
 			}
         	if(from <0){
         		from=0;
         	}
         	
         	if(size <0){
         		size=100;
         	}
         	
         	if((from+size)>MAX_RESULT){
     			size=MAX_RESULT-from;
     		}
         	
         		SearchRequestBuilder responsebuilder = client.prepareSearch(index_name).setTypes(index_type) ;
         		SearchResponse myresponse=responsebuilder.setQuery(                
                        //query_string查询 （所有字段内容包含以下文本的）
                        QueryBuilders.queryStringQuery(text)
                        )  
                        .setFrom(from).setSize(size)
//                        .setExplain(true)
                        .execute().actionGet();
    	
         		jsonMap=getHitsMap(myresponse.getHits());
         		 
    	 } catch (Exception e) {
             e.printStackTrace();
         }
    	 return jsonMap;
    }
    
    
    
    public static Map<String,String> selectQueryTop100(String index_name,String index_type,String text) {
    	Map<String,String> jsonMap= null;
    	
    	 try {
         	
    	
         		jsonMap=selectQuery(index_name, index_type, text, 0, 100);
         		 
    	 } catch (Exception e) {
             e.printStackTrace();
         }
    	 return jsonMap;
    }
    
    public static Map<String,String> selectSimpleQuery(String index_name,String index_type,String text,int from,int size) {
    	Map<String,String> jsonMap= null;
    	
    	 try {
         	if(client==null){
 				getConnection();//获取连接
 			}
         	if(from <0){
         		from=0;
         	}
         	
         	if(size <0){
         		size=100;
         	}	
         	if((from+size)>MAX_RESULT){
     			size=MAX_RESULT-from;
     		}
         	
         		SearchRequestBuilder responsebuilder = client.prepareSearch(index_name).setTypes(index_type) ;
         		SearchResponse myresponse=responsebuilder.setQuery(                
                        //8.simple_query_string查询,个人觉得与query_string没太大区别，感兴趣的可以深入研究
                        QueryBuilders.simpleQueryStringQuery(text)
                        )  
                        .setFrom(from).setSize(size)
//                        .setExplain(true)
                        .execute().actionGet();
    	
         		jsonMap=getHitsMap(myresponse.getHits());
         		 
    	 } catch (Exception e) {
             e.printStackTrace();
         }
    	 return jsonMap;
    }
   
    
    
    public static Map<String,String> selectSimpleQueryTop100(String index_name,String index_type,String text) {
    	Map<String,String> jsonMap= null;
    	
    	 try {
         	
    	
         		jsonMap=selectSimpleQuery(index_name, index_type, text, 0, 100);
         		 
    	 } catch (Exception e) {
             e.printStackTrace();
         }
    	 return jsonMap;
    }
   
    /**
     * 前缀查询 
     */
    public static Map<String,String> selectPrefixQuery(String index_name,String index_type,String key,String prefix,int from,int size) {
    	Map<String,String> jsonMap= null;
    	
    	 try {
         	if(client==null){
 				getConnection();//获取连接
 			}
         	if(from <0){
         		from=0;
         	}
         	
         	if(size <0){
         		size=100;
         	}	
         	if((from+size)>MAX_RESULT){
     			size=MAX_RESULT-from;
     		}
         	
         		SearchRequestBuilder responsebuilder = client.prepareSearch(index_name).setTypes(index_type) ;
         		 
         	    SearchResponse myresponse=responsebuilder.setQuery(                
         	            //8.前缀查询 （一个汉字，字符小写）
//         	            QueryBuilders.prefixQuery("name", "云")
         	            QueryBuilders.prefixQuery(key, prefix)
         	            )  
         	            .setFrom(from).setSize(size)
//         	            .setExplain(true)
         	            .execute().actionGet();
    	
         		jsonMap=getHitsMap(myresponse.getHits());
         		 
    	 } catch (Exception e) {
             e.printStackTrace();
         }
    	 return jsonMap;
    }
    
   
    
    public static Map<String,String> selectPrefixQueryTop100(String index_name,String index_type,String key,String prefix) {
    	Map<String,String> jsonMap= null;
    	
    	 try {
         	
         		jsonMap=selectPrefixQuery(index_name, index_type, key, prefix, 0, 100);
         		 
    	 } catch (Exception e) {
             e.printStackTrace();
         }
    	 return jsonMap;
    }
    
    /**
     * 使用模糊查询匹配文档查询
     * @param index_name
     * @param index_type
     * @param key
     * @param fuzzy
     * @return
     */
    public static Map<String,String> selectFuzzyQuery(String index_name,String index_type,String key,String fuzzy,int from,int size) {
    	Map<String,String> jsonMap= null;
    	
    	 try {
         	if(client==null){
 				getConnection();//获取连接
 			}
         	if(from <0){
         		from=0;
         	}
         	
         	if(size <0){
         		size=100;
         	}	
         	if((from+size)>MAX_RESULT){
     			size=MAX_RESULT-from;
     		}
         	
         		SearchRequestBuilder responsebuilder = client.prepareSearch(index_name).setTypes(index_type) ;
         		 
         		SearchResponse myresponse=responsebuilder.setQuery(      
                        //9.fuzzy_like_this，fuzzy_like_this_field，fuzzy查询
                        //fuzzyQuery:使用模糊查询匹配文档查询
                        QueryBuilders.fuzzyQuery(key, fuzzy)
                        )  
                        .setFrom(from).setSize(size)
//                        .setExplain(true)
                        .execute().actionGet();
    	
         		jsonMap=getHitsMap(myresponse.getHits());
         		 
    	 } catch (Exception e) {
             e.printStackTrace();
         }
    	 return jsonMap;
    }
    
    
    
    public static Map<String,String> selectFuzzyQueryTop100(String index_name,String index_type,String key,String fuzzy) {
    	Map<String,String> jsonMap= null;
    	
    	 try {
         		jsonMap=selectFuzzyQuery(index_name, index_type, key, fuzzy, 0, 100);
         		 
    	 } catch (Exception e) {
             e.printStackTrace();
         }
    	 return jsonMap;
    }
   
    /**
     * 通配符查询 ?用来匹配任意字符，*用来匹配零个或者多个字符
     * @param index_name
     * @param index_type
     * @param key
     * @param wildcard
     * @return
     */
    public static Map<String,String> selectWildcardQuery(String index_name,String index_type,String key,String wildcard,int from,int size) {
    	Map<String,String> jsonMap= null;
    	
    	 try {
         	if(client==null){
 				getConnection();//获取连接
 			}
         	if(from <0){
         		from=0;
         	}
         	
         	if(size <0){
         		size=100;
         	}	
         	if((from+size)>MAX_RESULT){
     			size=MAX_RESULT-from;
     		}
         	
         		SearchRequestBuilder responsebuilder = client.prepareSearch(index_name).setTypes(index_type) ;
         		 
         		SearchResponse myresponse=responsebuilder.setQuery(      
                        
                        //10.通配符查询 ?用来匹配任意字符，*用来匹配零个或者多个字符
//                        QueryBuilders.wildcardQuery("name", "岳*")//"?ue*"
                        QueryBuilders.wildcardQuery(key, wildcard)
                        )  
                        .setFrom(from).setSize(size)
//                        .setExplain(true)
                        .execute().actionGet();
    	
         		jsonMap=getHitsMap(myresponse.getHits());
         		 
    	 } catch (Exception e) {
             e.printStackTrace();
         }
    	 return jsonMap;
    }
    
    
    
    public static Map<String,String> selectWildcardQueryTop100(String index_name,String index_type,String key,String wildcard) {
    	Map<String,String> jsonMap= null;
    	
    	 try {
         	
         		jsonMap=selectWildcardQuery(index_name, index_type, key, wildcard, 0, 100);
         		 
    	 } catch (Exception e) {
             e.printStackTrace();
         }
    	 return jsonMap;
    }
    
    
    public static Map<String,String> selectRangeQuery(String index_name,String index_type,String key,int  gt,int lt,int from,int size) {
    	Map<String,String> jsonMap= null;
    	
    	 try {
         	if(client==null){
 				getConnection();//获取连接
 			}
         	if(from <0){
         		from=0;
         	}
         	
         	if(size <0){
         		size=100;
         	}	
         	if((from+size)>MAX_RESULT){
     			size=MAX_RESULT-from;
     		}
         	
         		SearchRequestBuilder responsebuilder = client.prepareSearch(index_name).setTypes(index_type) ;
         		 
         		SearchResponse myresponse=responsebuilder.setQuery(      
                        /**
                         * range过滤允许我们按照指定范围查找一批数据

范围操作符包含：

    gt :: 大于
    gte:: 大于等于
    lt :: 小于
    lte:: 小于等于

                         */
                        //12.rang查询
                        QueryBuilders.rangeQuery(key).gt(gt).lt(lt)                
                        )  
                        .setFrom(from).setSize(size).addSort("id", SortOrder.ASC)
//                        .setExplain(true)
                        .execute().actionGet();
    	
         		jsonMap=getHitsMap(myresponse.getHits());
         		 
    	 } catch (Exception e) {
             e.printStackTrace();
         }
    	 return jsonMap;
    }
    
    
    
    public static Map<String,String> selectRangeQueryTop100(String index_name,String index_type,String key,int  gt,int lt) {
    	Map<String,String> jsonMap= null;
    	
    	 try {
         		jsonMap=selectRangeQuery(index_name, index_type, key, gt, lt, 0, 100);
    	 } catch (Exception e) {
             e.printStackTrace();
         }
    	 return jsonMap;
    }
    
    /**
     * 正则表达式查询
     * @param index_name
     * @param index_type
     * @param key
     * @param regexp
     * @return
     */
    public static Map<String,String> selectRegexpQuery(String index_name,String index_type,String key,String regexp,int from,int size) {
    	Map<String,String> jsonMap= null;
    	
    	 try {
         	if(client==null){
 				getConnection();//获取连接
 			}
         	if(from <0){
         		from=0;
         	}
         	
         	if(size <0){
         		size=100;
         	}	
         	if((from+size)>MAX_RESULT){
     			size=MAX_RESULT-from;
     		}
         	
         		SearchRequestBuilder responsebuilder = client.prepareSearch(index_name).setTypes(index_type) ;
         		 
         		SearchResponse myresponse=responsebuilder.setQuery(      
                        
                        //14.正则表达式查询                    
                        QueryBuilders.regexpQuery(key, regexp)                
                        )  
                        .setFrom(from).setSize(size)
//                        .setExplain(true)
                        .execute().actionGet();
         		jsonMap=getHitsMap(myresponse.getHits());
         		 
    	 } catch (Exception e) {
             e.printStackTrace();
         }
    	 return jsonMap;
    }
    
    
    
    
    public static Map<String,String> selectRegexpQueryTop100(String index_name,String index_type,String key,String regexp) {
    	Map<String,String> jsonMap= null;
    	
    	 try {
         	
         		jsonMap=selectRegexpQuery(index_name, index_type, key, regexp, 0, 100);
         		 
    	 } catch (Exception e) {
             e.printStackTrace();
         }
    	 return jsonMap;
    }
    
	/************************************
   	 * select end
   	 ******************************************/
    public static Map<String,String> getHitsMap(SearchHits hits) {
    	Map<String,String> jsonMap= null;
    	
         		if(hits!=null &&hits.getHits()!=null&& hits.getHits().length>0){
         			 jsonMap = new HashMap<String, String>();
         			for (int i = 0; i < hits.getHits().length; i++) { 
         	          // System.out.println(hits.getHits()[i].getId()+":"+hits.getHits()[i].getSourceAsString());
         	          jsonMap.put(hits.getHits()[i].getId(), hits.getHits()[i].getSourceAsString());
         	         } 
         		}
         		 
    	
    	 return jsonMap;
    }


    
	public static void main(String[] args) {
		
		info();
		
		List<EsIndexSource> esIndexSourceList = new ArrayList<EsIndexSource>();
		esIndexSourceList.add(new EsIndexSource("code", "text", true, false));
		esIndexSourceList.add(new EsIndexSource("name", "keyword", true, false));
		esIndexSourceList.add(new EsIndexSource("info", "keyword", true, false));
		esIndexSourceList.add(new EsIndexSource("content", "text", true, true));
		esIndexSourceList.add(new EsIndexSource("my_title", "text", true, true));
		esIndexSourceList.add(new EsIndexSource("you_title", "text", true, true));
		esIndexSourceList.add(new EsIndexSource("isDelete", "text", true, true));
		esIndexSourceList.add(new EsIndexSource("age", "long", true, true));

		// createIndex("test2", "t2",esIndexSourceList);

		// indexStats("test2");

		Map<String, Object> sourceMap = new HashMap<String, Object>();
		sourceMap.put("code", "06");
		sourceMap.put("name", "最快的技术");
		sourceMap.put("info", "美丽大武汉");
		sourceMap.put("content", "美丽大武汉");
		sourceMap.put("my_title", "356thjmkj345");
		sourceMap.put("you_title", "4gfjgfjg4523");
		sourceMap.put("isDelete", false);
		sourceMap.put("age", 21);

		//System.out.println(saveMapReturnId("test3", "t4", sourceMap));
	}

}
