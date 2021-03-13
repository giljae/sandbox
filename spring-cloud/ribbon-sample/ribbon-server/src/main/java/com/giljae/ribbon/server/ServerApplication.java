package com.giljae.ribbon.server;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.env.Environment;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;

/**
 * Ribbon Client 테스트를 위한 서버 클래스
 * 
 * @author giljae
 *
 */
@SpringBootApplication
@RestController
public class ServerApplication {

	@Autowired
	private Environment environment;

	/**
	 * Normal case
	 * 
	 * @return
	 */
	@RequestMapping("/normal")
	public String normal() {
		String port = environment.getProperty("local.server.port");
		String nowDate = new Date().toString();
		System.out.println("[Normal] " + port + " : (" + nowDate + ")");

		return "status : ok";
	}

	/**
	 * Waiting case
	 * 
	 * @return
	 */
	@RequestMapping("/waiting")
	public String waiting() {
		String port = environment.getProperty("local.server.port");
		String nowDate = new Date().toString();
		System.out.println("[Waiting] " + port + " : Waiting case (" + nowDate + ")");
		try {
			Thread.sleep(20000); // 20sec
		} catch (InterruptedException ie) {
			ie.printStackTrace();
		}

		return "status : ok";
	}

	public static void main(String[] args) {
		SpringApplication.run(ServerApplication.class, args);
	}
}
