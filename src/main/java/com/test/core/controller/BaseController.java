package com.test.core.controller;

import org.springframework.beans.factory.annotation.Autowired;

import com.test.core.service.RedisService;

public class BaseController {
	 @Autowired
	 public RedisService redisService;// 由Spring容器注入一个RedisService实例
	
	 
}
