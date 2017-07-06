package com.ravi.sparkspring.poc;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.ravi.sparkspring.poc.service.SparkJobMediatorService;

@SpringBootApplication
public class PocApplication implements CommandLineRunner {

    @Autowired
    SparkJobMediatorService sparkJobMediatorService;
    
	public static void main(String[] args) {
		SpringApplication.run(PocApplication.class, args);
	}

	
    @Override
    public void run(String... args) throws Exception {
    	
    	//sparkJobMediatorService.testJobWordCount();
    	//sparkJobMediatorService.testlineCount();
    	//sparkJobMediatorService.testCrimeData();
    	//sparkJobMediatorService.testOlympicsData();
    	
    	//sparkJobMediatorService.testDailyShowData();  issue in this job date format issue
    	
    	//sparkJobMediatorService.testAnalyseTravelData();
    	//sparkJobMediatorService.testAnalyseWeatherData();
    	
    	//sparkJobMediatorService.testAnalyseSocialMediaData();
    	
    	//sparkJobMediatorService.testAnalyseSensorData();
    	
    	sparkJobMediatorService.testSocialNumberData();
    } 
}
