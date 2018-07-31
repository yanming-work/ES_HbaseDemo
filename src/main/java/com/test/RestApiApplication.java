package com.test;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
@Configuration
@SpringBootApplication
@ComponentScan({"com.test"})
@EnableAutoConfiguration(exclude={DataSourceAutoConfiguration.class})//不链接数据库
public class RestApiApplication {  

    public static void main(String[] args) {
    	System.setProperty("hadoop.home.dir", "D:\\hadoop-2.8.4");
        SpringApplication.run(RestApiApplication.class, args);
        
    }

    @Bean
    public static PropertySourcesPlaceholderConfigurer placeholderConfigurer() {
        PropertySourcesPlaceholderConfigurer c = new PropertySourcesPlaceholderConfigurer();
        c.setIgnoreUnresolvablePlaceholders(true);
        return c;
    }
}

