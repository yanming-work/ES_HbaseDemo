package com.test.core.config;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class HbaseConfig {

	private static final Logger LOGGER = LoggerFactory.getLogger(HbaseConfig.class);

	@Value("${spring.data.hbase.master}")
	private String hBaseMaster;
	 
    @Value("${spring.data.hbase.zookeeper.quorum}")
    private String quorum;

    @Value("${spring.data.hbase.zookeeper.property.clientPort}")
    private String clientPort;

    @Value("${spring.data.zookeeper.znode.parent}")
    private String znode;

    private static org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();

    public static Connection connection;

	@Bean(name = "hbaseConnection")
    public Connection hbaseConnection(){

		LOGGER.info("Hbase初始化开始。。。。。");
        try {
        	conf = HBaseConfiguration.create();
        	conf.set("hbase.zookeeper.quorum", quorum);  
        	conf.set("hbase.zookeeper.property.clientPort", clientPort);  
            connection = ConnectionFactory.createConnection(conf);
            if(connection!=null){
            	 LOGGER.info("获取Hbase connectiont连接成功！");
            }
           
        } catch (IOException e) {
            e.printStackTrace ();
            LOGGER.error("获取Hbase connectiont连接失败！");
        }
        
        return connection;
    }

	
	
	
	
}
