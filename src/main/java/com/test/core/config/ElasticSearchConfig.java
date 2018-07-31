package com.test.core.config;

import java.net.InetAddress;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @Configuration用于定义配置类，可替换xml配置文件
 */
@Configuration
public class ElasticSearchConfig {

	private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchConfig.class);

	/**
	 * elk集群地址
	 */
	@Value("${spring.data.elasticsearch.host}")
	private String hostName;

	/**
	 * 端口
	 */
	@Value("${spring.data.elasticsearch.port}")
	private String port;

	/**
	 * 集群名称
	 */
	@Value("${spring.data.elasticsearch.cluster-name}")
	private String clusterName;

	/**
	 * 连接池
	 */
	@Value("${spring.data.elasticsearch.pool}")
	private String poolSize;

	@Value("${spring.data.elasticsearch.max-result-window}")
	public static String maxResultWindow;

	/**
	 * Bean name default 函数名字
	 * 
	 * @return
	 */
	@Bean(name = "transportClient")
	public TransportClient transportClient() {
		LOGGER.info("Elasticsearch初始化开始。。。。。");
		TransportClient transportClient = null;
		try {
			// 配置信息
			Settings esSetting = Settings.builder().put("cluster.name", clusterName) // 集群名字
					.put("client.transport.sniff", true)// 增加嗅探机制，找到ES集群
					.put("thread_pool.search.size", Integer.parseInt(poolSize))// 增加线程池个数，暂时设为5
					.build();
			/**
			// 配置信息Settings自定义
			transportClient = new PreBuiltTransportClient(esSetting);
			//TransportAddress transportAddress = new InetSocketTransportAddress(InetAddress.getByName(hostName),Integer.valueOf(port));
			TransportAddress transportAddress = new TransportAddress(InetAddress.getByName(hostName),Integer.valueOf(port));
			
			transportClient.addTransportAddresses(transportAddress);
			**/
			// 创建client
			transportClient = new PreBuiltTransportClient(esSetting)
					.addTransportAddresses(new InetSocketTransportAddress(InetAddress.getByName(hostName), Integer.valueOf(port)));
			
		
			
		} catch (Exception e) {
			LOGGER.error("elasticsearch TransportClient create error!!", e);
		}
		return transportClient;
	}

}
