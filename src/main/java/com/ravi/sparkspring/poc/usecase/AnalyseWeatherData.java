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
public class AnalyseWeatherData {

    @Autowired
    JavaSparkContext javaSparkContext;
    
    @Autowired
    ApplicationConfiguration applicationConfiguration;
    
    public void analyseWeatherData(){
    	
    	//https://acadgild.com/blog/spark-use-case-weather-data-analysis/
    	
    	String inputFile = applicationConfiguration.getWeatherDataAnalysisInput();

    	analyseData(javaSparkContext, inputFile);
		
    }
    
    private static void analyseData(JavaSparkContext javaSparkContext2, String inputFile){
    	
		JavaPairRDD<String, Double> resultRDD = javaSparkContext2.textFile(inputFile)
				.map(new Function<String, Tuple2<String, Double>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Double> call(String v1) throws Exception {
						String splits[] = v1.split(",");
						return new Tuple2<String, Double>(splits[0], 
								(Double.parseDouble(splits[3]) * 0.1 * (9.0 / 5.0)) + 32.0);
					}
				}).mapToPair(new PairFunction<Tuple2<String, Double>, String, Double>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Double> call(Tuple2<String, Double> t) throws Exception {
						return new Tuple2<String, Double>(t._1, t._2);
					}
				}).reduceByKey(new Function2<Double, Double, Double>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Double call(Double v1, Double v2) throws Exception {
						return Math.min(v1, v2);
					}
				});
		
				Map<String, Double> result = resultRDD.sortByKey(false).repartition(1).collectAsMap();
		
				System.out.println("########## minimum temperature observed ######## " );
				
		        for (String key : result.keySet()) {
		        	System.out.println("Key :::: " + key);
		        	System.out.println("Value :::: " + result.get(key));
		        }	
    	}
	
}
