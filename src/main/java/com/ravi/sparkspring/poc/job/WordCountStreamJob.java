package com.ravi.sparkspring.poc.job;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

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
    				socketTextStream("localhost", 4545, StorageLevel.DISK_ONLY());
		
    	JavaDStream<String> words = stream.flatMap(flatMappingForSpark);

		// Count each word in file with count 1
    	JavaPairDStream<String, Integer> pairs = words.mapToPair(initiateMap);

		//Reducer function
    	JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(reduce);

		//Print on console, For integration use it as return type or callback/future observable object
		wordCounts.print();

		javaStreamingContext.start();
		javaStreamingContext.awaitTermination();

    }
    
    static FlatMapFunction<String, String> flatMappingForSpark = new FlatMapFunction<String, String>() {

		private static final long serialVersionUID = 1L;

		@SuppressWarnings("unchecked")
		@Override
		public Iterator<String> call(String input) throws Exception {
			
			List <String> result = Arrays.asList(input.split(" "));	
			return result.listIterator();		
		}
	};
	
    static PairFunction<String, String, Integer> initiateMap = new PairFunction<String, String, Integer>() {
		
		private static final long serialVersionUID = 1L;
		
		@Override
		public Tuple2<String, Integer> call(String s) {
			return new Tuple2<String, Integer>(s, 1);
		}
	};
	
    static Function2<Integer, Integer, Integer> reduce = new Function2<Integer, Integer, Integer>() {
		
		private static final long serialVersionUID = 1L;
		
		@Override
		public Integer call(Integer value1, Integer value2) throws Exception {
			return value1 + value2;
		}		
	};
}
