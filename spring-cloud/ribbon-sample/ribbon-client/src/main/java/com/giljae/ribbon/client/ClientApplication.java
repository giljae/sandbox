package com.giljae.ribbon.client;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.client.loadbalancer.LoadBalanced;
import org.springframework.cloud.netflix.ribbon.RibbonClient;
import org.springframework.cloud.netflix.ribbon.RibbonClientHttpRequestFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import com.giljae.ribbon.client.configuration.Configuration;
import com.netflix.client.config.IClientConfig;
import com.netflix.loadbalancer.AvailabilityFilteringRule;
import com.netflix.loadbalancer.DynamicServerListLoadBalancer;
import com.netflix.loadbalancer.IPing;
import com.netflix.loadbalancer.IRule;
import com.netflix.loadbalancer.PingUrl;

/**
 * Ribbon 테스트를 위한 Ribbon Client 클래스
 * 
 * @author giljae
 *
 */
@RestController
@SpringBootApplication
@RibbonClient(name = "ribbonClient", configuration = Configuration.class)
public class ClientApplication {
	@Value("${ribbon.ConnectionTimeout:1000}")
	int connectionTimeout;

	@Value("${ribbon.ReadTimeout:1000}")
	int readTimeout;

	@LoadBalanced
	@Bean
	RestTemplate restTemplate() {
		return new RestTemplateBuilder().setConnectTimeout(connectionTimeout).setReadTimeout(readTimeout).build();
	}

	@Autowired
	private RestTemplate restTemplate;

	@RequestMapping("/normal")
	public String normal() {
		String testString = null;

		testString = this.restTemplate.getForObject("http://ribbon-server/normal", String.class);

		return testString;
	}

	@RequestMapping("/waiting")
	public String waiting() {
		String testString = null;

		testString = this.restTemplate.getForObject("http://ribbon-server/waiting", String.class);

		return testString;
	}

	public static void main(String[] args) {
		SpringApplication.run(ClientApplication.class, args);
	}
}