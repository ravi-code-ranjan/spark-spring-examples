package com.ravi.sparkspring.poc.job;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import scala.Tuple2;

import com.ravi.sparkspring.poc.config.ApplicationConfiguration;

@Component
public class WordCountJob {
    
    @Autowired
    JavaSparkContext javaSparkContext;
    
    @Autowired
    ApplicationConfiguration applicationConfiguration;

    public void count() {

        String filePath = applicationConfiguration.getFileWordCountPath();
        JavaRDD<String> textFile = javaSparkContext.textFile(filePath);
        
        /**
         * Approach 1
         */
        
        //word count
        JavaPairRDD<String, Integer> counts = textFile
            .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
            .mapToPair(word -> new Tuple2<>(word, 1))
            .reduceByKey((a, b) -> a + b);

        Map<String, Integer> allCount = counts.collectAsMap();
   
        System.out.println("***************** Approcah 1 *****************");
        
        for (String key : allCount.keySet()) {
        	System.out.println("Word :::: " + key);
        	System.out.println("Count :::: " + allCount.get(key));
        }
        
        /**
         * Approach 2
         */
        
        JavaRDD<String> textFile2 = javaSparkContext.textFile(filePath);
        
        	//flattening the list
        	JavaRDD<String> words = textFile2.flatMap(new FlatMapFunction<String, String>() {

				private static final long serialVersionUID = 1L;

				@SuppressWarnings("unchecked")
				@Override
      			public Iterator<String> call(String input) throws Exception {
      				return (Iterator<String>) Arrays.asList(input.split(" "));			
      			}
      		});

      		// Count each word in file with count 1
      		JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
      			
    			private static final long serialVersionUID = 1L;
    			
      			@Override
      			public Tuple2<String, Integer> call(String s) {
      				return new Tuple2<String, Integer>(s, 1);
      			}
      		});

      		//Reducer function
      		JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
      			
    			private static final long serialVersionUID = 1L;
      			
      			@Override
      			public Integer call(Integer value1, Integer value2) throws Exception {
      				// TODO Auto-generated method stub
      				return value1 + value2;
      			}		
      		});
      		
      		Map<String, Integer> allCount2 = wordCounts.collectAsMap();
      		
      		
            System.out.println("***************** Approcah 2 *****************");
            
            for (String key : allCount2.keySet()) {
            	System.out.println("Word :::: " + key);
            	System.out.println("Count :::: " + allCount.get(key));
            }

    }
    
}
