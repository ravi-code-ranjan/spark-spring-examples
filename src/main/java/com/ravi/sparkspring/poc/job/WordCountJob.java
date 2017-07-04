package com.ravi.sparkspring.poc.job;

import java.util.Arrays;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import scala.Tuple2;

import com.ravi.sparkspring.poc.config.ApplicationConfiguration;

@Component
public class WordCountJob {

    @Autowired
    private SparkSession sparkSession;
    
    @Autowired
    JavaSparkContext javaSparkContext;
    
    @Autowired
    ApplicationConfiguration applicationConfiguration;

    public void count() {

        String filePath = applicationConfiguration.getFileWordCountPath();
        JavaRDD<String> textFile = javaSparkContext.textFile(filePath);
        
        //word count
        JavaPairRDD<String, Integer> counts = textFile
            .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
            .mapToPair(word -> new Tuple2<>(word, 1))
            .reduceByKey((a, b) -> a + b);

        Map<String, Integer> allCount = counts.collectAsMap();
        
        for (String key : allCount.keySet()) {
        	System.out.println("Word :::: " + key);
        	System.out.println("Count :::: " + allCount.get(key));
        }
        
        System.out.println(counts);
    }
    
}
