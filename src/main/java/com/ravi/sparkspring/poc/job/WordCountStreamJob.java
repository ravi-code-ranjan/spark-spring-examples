package com.ravi.sparkspring.poc.job;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import scala.Tuple2;

import com.ravi.sparkspring.poc.config.ApplicationConfiguration;

@Component
public class WordCountStreamJob {
    
    @Autowired
    JavaStreamingContext javaStreamingContext;
    
    @Autowired
    ApplicationConfiguration applicationConfiguration;
    
    
    public void countInStream() throws InterruptedException {

		JavaReceiverInputDStream<String> stream = javaStreamingContext.
				socketTextStream(applicationConfiguration.getSparkStreamHostedService(),
								 applicationConfiguration.getSparkStreamHostedPort(), StorageLevel.DISK_ONLY());
		
    	//flattening the list
		JavaDStream<String> words = stream.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = 1L;

			@SuppressWarnings("unchecked")
			@Override
			public Iterator<String> call(String input) throws Exception {
				return (Iterator<String>) Arrays.asList(input.split(" "));			
			}
		});

		// Count each word in each batch
		JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<String, Integer>(s, 1);
			}
		});

		//Reducer function
		JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer value1, Integer value2) throws Exception {
				// TODO Auto-generated method stub
				return value1 + value2;
			}		
		});

		// Print the first ten elements of each RDD generated in this DStream to
		// the console
		wordCounts.print();

		javaStreamingContext.start();
		javaStreamingContext.awaitTermination();

    }
}
