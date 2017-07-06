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
public class AnalyseSocialMediaData {

    @Autowired
    JavaSparkContext javaSparkContext;
    
    @Autowired
    ApplicationConfiguration applicationConfiguration;
    
    public void analyseSocialMedia(){
  
    	//https://acadgild.com/blog/spark-use-case-social-media-analysis/
    	
    	String inputFile = applicationConfiguration.getSocailFrndsDataAnalysisInput();
    	
    	analyseData(javaSparkContext, inputFile);
    }
    
    private static void analyseData(JavaSparkContext javaSparkContext2,
			String inputFile) {
		
    	
    	JavaPairRDD<Integer, Double> resultRdd = javaSparkContext2.textFile(inputFile)
				.map(new Function<String, Tuple2<Integer, Integer>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Integer, Integer> call(String v1) throws Exception {
						String splits[] = v1.split(",");
						return new Tuple2<Integer, Integer>(Integer.parseInt(splits[2]), Integer.parseInt(splits[3]));
					}
				}).mapToPair(new PairFunction<Tuple2<Integer, Integer>, Integer, Tuple2<Integer, Integer>>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Integer, Tuple2<Integer, Integer>> call(Tuple2<Integer, Integer> t) throws Exception {
						return new Tuple2<Integer, Tuple2<Integer, Integer>>(t._1, new Tuple2<Integer, Integer>(t._2, 1));
					}
				}).reduceByKey(new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> v1, Tuple2<Integer, Integer> v2) throws Exception {
						return new Tuple2<Integer, Integer>(v1._1 + v2._1, v1._2 + v2._2);
					}
				}).mapToPair(new PairFunction<Tuple2<Integer, Tuple2<Integer, Integer>>, Integer, Double>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Integer, Double> call(Tuple2<Integer, Tuple2<Integer, Integer>> t) throws Exception {
						return new Tuple2<Integer, Double>(t._1, t._2._1*1.0 / t._2._2() * 1.0);
					}
				});
    	
		
		Map<Integer, Double> result = resultRdd.sortByKey(true).repartition(1).collectAsMap();
		
		System.out.println("########## average number of friends based on their age, ######## " );
		
        for (Integer key : result.keySet()) {
        	System.out.println("Key :::: " + key);
        	System.out.println("Value :::: " + result.get(key));
        }	
        
	}

}
