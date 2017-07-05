package com.ravi.sparkspring.poc.config;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.env.Environment;

@Configuration
@PropertySource("classpath:application.properties")
public class ApplicationConfiguration {

	@Autowired
    private Environment env;

    @Value("${app.name}")
    private String appName;

    @Value("${spark.home}")
    private String sparkHome;

    @Value("${master.uri:local}")
    private String masterUri;
    
    @Value("${spark.streamduration}")
    private Long sparkStreamDuration;
    
    @Value("${spark.streamhostservice}")
    private String sparkStreamHostedService;
    
    @Value("${spark.streamhostport}")
    private Integer sparkStreamHostedPort;
    
    @Value("${spark.streamstoragelevel}")
    private Long sparkStreamStorageLevel;
    
    @Value("${app.word}")
    private String inputString;
    
    @Value("${file.path}")
    private String fileWordCountPath;

    @Bean
    public SparkConf sparkConf() {
        SparkConf sparkConf = new SparkConf()
                .setAppName(appName)
                .setSparkHome(sparkHome)
                .setMaster(masterUri);

        return sparkConf;
    }

    @Bean
    public JavaSparkContext javaSparkContext() {
        return new JavaSparkContext(sparkConf());
    }

    @Bean
    public JavaStreamingContext javaStreamingContext() {
        return new JavaStreamingContext(sparkConf(), 
        		new Duration(sparkStreamDuration));
    }
    
    @Bean
    public SparkSession sparkSession() {
        return SparkSession
                .builder()
                .sparkContext(javaSparkContext().sc())
                .appName("Java Spark Ravi")
                .getOrCreate();
    }
    
    @Bean
    public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
        return new PropertySourcesPlaceholderConfigurer();
}

	public String getInputString() {
		return inputString;
	}

	public String getFileWordCountPath() {
		return fileWordCountPath;
	}

	public String getSparkStreamHostedService() {
		return sparkStreamHostedService;
	}

	public Integer getSparkStreamHostedPort() {
		return sparkStreamHostedPort;
	}

	public Long getSparkStreamStorageLevel() {
		return sparkStreamStorageLevel;
	}
}
