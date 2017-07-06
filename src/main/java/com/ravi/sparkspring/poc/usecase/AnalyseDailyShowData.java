package com.ravi.sparkspring.poc.usecase;

import java.text.SimpleDateFormat;
import java.util.Date;
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
public class AnalyseDailyShowData {

    @Autowired
    JavaSparkContext javaSparkContext;
    
    @Autowired
    ApplicationConfiguration applicationConfiguration;
    

    private static final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("MM/dd/yy");

    public void analyseDailyShow(){
    	
    	//https://acadgild.com/blog/spark-use-case-daily-show/

    	String inputFile = applicationConfiguration.getDailyShowsAnalysisInput();
    	
    	analyseData(javaSparkContext, inputFile);
    }
    
    
    private static void analyseData(JavaSparkContext javaSparkContext2, String inputFile){
    	
    	JavaPairRDD<Integer, String> rdd = javaSparkContext2.textFile(inputFile)
				.map(new Function<String, Tuple2<String, Date>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Date> call(String v1) throws Exception {
						String splits[] = v1.split(",");
						return new Tuple2<String, Date>(splits[1], simpleDateFormat.parse(splits[2]));
					}
				}).filter(new Function<Tuple2<String, Date>, Boolean>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(Tuple2<String, Date> v1) throws Exception {
						return v1._2.after(simpleDateFormat.parse("1/11/99")) && 
							   v1._2().before(simpleDateFormat.parse("6/11/99"));
					}
				}).mapToPair(new PairFunction<Tuple2<String, Date>, String, Integer>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Integer> call(Tuple2<String, Date> t) throws Exception {
						return new Tuple2<String, Integer>(t._1, 1);
					}
				}).reduceByKey(new Function2<Integer, Integer, Integer>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(Integer v1, Integer v2) throws Exception {
						return v1 + v2;
					}
				}).mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
						return new Tuple2<Integer, String>(t._2, t._1);
					}
				});
    	
		Map<Integer, String> result = rdd.sortByKey(true).repartition(1).collectAsMap();
		
		System.out.println("########## Sorted Record of knowledge, ocupation ######## " );
		
        for (Integer key : result.keySet()) {
        	System.out.println("Word :::: " + key);
        	System.out.println("Count :::: " + result.get(key));
        }	
    }
}
