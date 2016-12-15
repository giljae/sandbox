package com.giljae.sandbox.swaggerspringboot;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@EnableAutoConfiguration
@ComponentScan
public class SwaggerSpringbootApplication {

	public static void main(String[] args) {
		SpringApplication.run(SwaggerSpringbootApplication.class, args);
	}
}
