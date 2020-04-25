package com.poc.sqs;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

@Configuration
public class WebMvcConfig extends WebMvcConfigurerAdapter {

	@Override
	public void addCorsMappings(CorsRegistry registry) {
		registry.addMapping("/*/*/*/*").allowedOrigins("*");
		registry.addMapping("*/**").allowedOrigins("*");
		registry.addMapping("/*/*/*").allowedOrigins("*");
		registry.addMapping("/*/*").allowedOrigins("*");
		registry.addMapping("/*").allowedOrigins("*");
	}

}