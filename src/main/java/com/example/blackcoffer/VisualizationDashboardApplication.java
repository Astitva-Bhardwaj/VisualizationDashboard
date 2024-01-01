package com.example.blackcoffer;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableBatchProcessing
public class VisualizationDashboardApplication {

	public static void main(String[] args) {
		SpringApplication.run(VisualizationDashboardApplication.class, args);
	}

}
