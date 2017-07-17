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
    private String sparkStreamStorageLevel;
    
    @Value("${crime.input}")
    private String crimeAnalysisInput;
    
    @Value("${olympics.input}")
    private String olympicsAnalysisInput;
    
    @Value("${dailyshow.input}")
    private String dailyShowsAnalysisInput;
    
    @Value("${traveldata.input}")
    private String travelDataAnalysisInput;
    
    @Value("${weatherdata.input}")
    private String weatherDataAnalysisInput;
    
    @Value("${socialmediadata.input}")
    private String socailFrndsDataAnalysisInput;
    
    @Value("${sensordata.input1}")
    private String sensorDataAnalysisInput;
    
    @Value("${sensordata.input2}")
    private String sensorDataAnalysisInput2;
    
    @Value("${numberdata.input1}")
    private String numberDataAnalysisInput1;
    
    @Value("${numberdata.input2}")
    private String numberDataAnalysisInput2;
    
    @Value("${app.word}")
    private String inputString;
    
    @Value("${file.path}")
    private String fileWordCountPath;
    
    JavaSparkContext javaSparkContext;

    @Bean
    public SparkConf sparkConf() {
    	SparkConf sparkconf = new SparkConf()
                .setAppName(appName)
                .setSparkHome(sparkHome)
                .setMaster(masterUri);
        sparkconf
        		.set("spark.driver.allowMultipleContexts", "true");
        
        return sparkconf;
    }

    @Bean
    public JavaSparkContext javaSparkContext() {
    	javaSparkContext = new JavaSparkContext(sparkConf());
        return javaSparkContext;
    }

    @Bean
    public JavaStreamingContext javaStreamingContext() {
        return new JavaStreamingContext(javaSparkContext, 
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

	public String getSparkStreamStorageLevel() {
		return sparkStreamStorageLevel;
	}

	public String getCrimeAnalysisInput() {
		return crimeAnalysisInput;
	}
	
	public String getOlympicsAnalysisInput() {
		return olympicsAnalysisInput;
	}

	public String getDailyShowsAnalysisInput() {
		return dailyShowsAnalysisInput;
	}

	public String getTravelDataAnalysisInput() {
		return travelDataAnalysisInput;
	}

	public String getWeatherDataAnalysisInput() {
		return weatherDataAnalysisInput;
	}

	public String getSocailFrndsDataAnalysisInput() {
		return socailFrndsDataAnalysisInput;
	}

	public String getSensorDataAnalysisInput() {
		return sensorDataAnalysisInput;
	}

	public String getSensorDataAnalysisInput2() {
		return sensorDataAnalysisInput2;
	}

	public String getNumberDataAnalysisInput1() {
		return numberDataAnalysisInput1;
	}

	public String getNumberDataAnalysisInput2() {
		return numberDataAnalysisInput2;
	}
}
