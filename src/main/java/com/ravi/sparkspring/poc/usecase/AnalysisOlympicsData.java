package com.ravi.sparkspring.poc.usecase;

import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import scala.Tuple2;

import com.ravi.sparkspring.poc.config.ApplicationConfiguration;

@Component
public class AnalysisOlympicsData {

    @Autowired
    JavaSparkContext javaSparkContext;
    
    @Autowired
    ApplicationConfiguration applicationConfiguration;
    
    public void analyseOlympics(){
    	
    	//https://acadgild.com/blog/spark-use-case-olympics-data-analysis/

    	findAllCountryMedalsGivenSports(javaSparkContext, applicationConfiguration, "Gymnastics");

		findMedalByYear(javaSparkContext, applicationConfiguration);

		findAllCountryMedals(javaSparkContext, applicationConfiguration, null);
    		
    }
    
    private static void findAllCountryMedalsGivenSports(
			JavaSparkContext javaSparkContext2,
			ApplicationConfiguration applicationConfiguration2, String sports) {
		
    	String inputFile = applicationConfiguration2.getOlympicsAnalysisInput();
		
		JavaPairRDD<String, Integer> rdd = javaSparkContext2.textFile(inputFile)
				.filter(new Function<String, Boolean>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(String v1) throws Exception {
						String splits[] = v1.split(",");
						return sports == null ? true : splits[5].equals(sports);
					}
				}).mapToPair(new PairFunction<String, String, Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Integer> call(String t) throws Exception {
						String splits[] = t.split(",");
						return new Tuple2<String, Integer>(splits[2],
								Integer.parseInt(splits[6]) + Integer.parseInt(splits[7]) + Integer.parseInt(splits[8]));
					}
				}).reduceByKey(new Function2<Integer, Integer, Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(Integer v1, Integer v2) throws Exception {
						return v1 + v2;
					}
				});
		
		Map<String, Integer> result = rdd.repartition(1).collectAsMap();
		
		System.out.println("########## For all country a given Sports " + sports +" #########");
		
        for (String key : result.keySet()) {
        	System.out.println("Word :::: " + key);
        	System.out.println("Count :::: " + result.get(key));
        }
		
	}

	private static void findMedalByYear(JavaSparkContext javaSparkContext, 
    		ApplicationConfiguration applicationConfiguration2) {
    	
    	String inputFile = applicationConfiguration2.getOlympicsAnalysisInput();
    			
		JavaPairRDD<String, Integer> rdd = javaSparkContext.textFile(inputFile)
				.filter(new Function<String, Boolean>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(String v1) throws Exception {
						String splits[] = v1.split(",");
						return splits[2].equalsIgnoreCase("India");
					}
				}).mapToPair(new PairFunction<String, String, Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Integer> call(String t) throws Exception {
						String splits[] = t.split(",");
						return new Tuple2<String, Integer>(splits[3],
								Integer.parseInt(splits[6]) + Integer.parseInt(splits[7]) + Integer.parseInt(splits[8]));
					}
				}).reduceByKey(new Function2<Integer, Integer, Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(Integer v1, Integer v2) throws Exception {
						return v1 + v2;
					}
				});
		
		Map<String, Integer> result = rdd.repartition(1).collectAsMap();
		
		System.out.println("########## All Medals in all years for India #########");
		
        for (String key : result.keySet()) {
        	System.out.println("Word :::: " + key);
        	System.out.println("Count :::: " + result.get(key));
        }
	}

	private static void findAllCountryMedals(JavaSparkContext javaSparkContext, 
			ApplicationConfiguration applicationConfiguration2, String sports) {
		
		String inputFile = applicationConfiguration2.getOlympicsAnalysisInput();
		
		JavaPairRDD<String, Integer> rdd = javaSparkContext.textFile(inputFile)
				.filter(new Function<String, Boolean>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(String v1) throws Exception {
						String splits[] = v1.split(",");
						return sports == null ? true : splits[5].equals(sports);
					}
				}).mapToPair(new PairFunction<String, String, Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Integer> call(String t) throws Exception {
						String splits[] = t.split(",");
						return new Tuple2<String, Integer>(splits[2],
								Integer.parseInt(splits[6]) + Integer.parseInt(splits[7]) + Integer.parseInt(splits[8]));
					}
				}).reduceByKey(new Function2<Integer, Integer, Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(Integer v1, Integer v2) throws Exception {
						return v1 + v2;
					}
				});
		
		Map<String, Integer> result = rdd.repartition(1).collectAsMap();
		
		System.out.println("########## For all country in all Sports #########");
		
        for (String key : result.keySet()) {
        	System.out.println("Word :::: " + key);
        	System.out.println("Count :::: " + result.get(key));
        }
}
}
