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
import scala.Tuple3;

import com.ravi.sparkspring.poc.config.ApplicationConfiguration;

@Component
public class AnalyseTravelData {

    @Autowired
    JavaSparkContext javaSparkContext;
    
    @Autowired
    ApplicationConfiguration applicationConfiguration;
    
    public void analyseTravelData(){
    	
    	//https://acadgild.com/blog/spark-use-case-travel-data-analysis/

    	String inputFile = applicationConfiguration.getTravelDataAnalysisInput();
    	
    	findTopSourceStation(javaSparkContext, inputFile);
    		
		findTopDestinationStation(javaSparkContext, inputFile);
		
		findTopSourceRevenueStation(javaSparkContext, inputFile);
    }

	private static void findTopSourceRevenueStation(
			JavaSparkContext javaSparkContext2, String inputFile) {
		
		JavaPairRDD<Integer, String> resultRDD = javaSparkContext2.textFile(inputFile)
		  .map(new Function<String, Tuple3<String, String, String>>() {
							private static final long serialVersionUID = 1L;

							@Override
							public Tuple3<String, String, String> call(String v1) throws Exception {
								String splits[] = v1.split("\t");
								return new Tuple3<String, String, String>(splits[2], splits[1], splits[3]);
							}
		}).filter(new Function<Tuple3<String,String,String>, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple3<String, String, String> v1) throws Exception {
				return v1._3().equals("1");
			}
		}).mapToPair(new PairFunction<Tuple3<String,String,String>, String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(Tuple3<String, String, String> t) throws Exception {
				return new Tuple2<String, Integer>(t._1(), Integer.parseInt(t._3()));
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				// TODO Auto-generated method stub
				return v1+v2;
			}
		}).mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
				return new Tuple2<Integer, String>(t._2,t._1);
			}
		});
		
		Map<Integer, String> result = resultRDD.sortByKey(false).repartition(1).collectAsMap();
		
		System.out.println("########## Top Source Revenue Station ######## " );
		
        for (Integer key : result.keySet()) {
        	System.out.println("Word :::: " + key);
        	System.out.println("Count :::: " + result.get(key));
        }	
		
	}

	private static void findTopDestinationStation(JavaSparkContext javaSparkContext2,
			String inputFile) {
		
		JavaPairRDD<Integer, String> resultRDD = javaSparkContext2.textFile(inputFile)
		  .map(new Function<String, Tuple3<String, String, String>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple3<String, String, String> call(String v1) throws Exception {
						String splits[] = v1.split("\t");
						return new Tuple3<String, String, String>(splits[2], splits[1], splits[3]);
					}
		}).mapToPair(new PairFunction<Tuple3<String,String,String>, String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(Tuple3<String, String, String> t) throws Exception {
				return new Tuple2<String, Integer>(t._2(), 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				// TODO Auto-generated method stub
				return v1+v2;
			}
		}).mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
				return new Tuple2<Integer, String>(t._2,t._1);
			}
		});
		
		Map<Integer, String> result = resultRDD.sortByKey(false).repartition(1).collectAsMap();
		
		System.out.println("########## Top Destination Station ######## " );
		
        for (Integer key : result.keySet()) {
        	System.out.println("Word :::: " + key);
        	System.out.println("Count :::: " + result.get(key));
        }	
		
		
	}

	private static void findTopSourceStation(JavaSparkContext javaSparkContext2,
			String inputFile) {
		
		
			JavaPairRDD<Integer, String> resultRDD = javaSparkContext2.textFile(inputFile)
				.map(new Function<String, Tuple3<String, String, String>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple3<String, String, String> call(String v1) throws Exception {
						String splits[] = v1.split("\t");
						return new Tuple3<String, String, String>(splits[2], splits[1], splits[3]);
					}
				}).filter(new Function<Tuple3<String,String,String>, Boolean>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(Tuple3<String, String, String> v1) throws Exception {
						return v1._3().equals("1");
					}
				}).mapToPair(new PairFunction<Tuple3<String,String,String>, String, Integer>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Integer> call(Tuple3<String, String, String> t) throws Exception {
						return new Tuple2<String, Integer>(t._1(), Integer.parseInt(t._3()));
					}
				}).reduceByKey(new Function2<Integer, Integer, Integer>() {
					
					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(Integer v1, Integer v2) throws Exception {
						// TODO Auto-generated method stub
						return v1+v2;
					}
				}).mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
						return new Tuple2<Integer, String>(t._2,t._1);
					}
				});
			
			Map<Integer, String> result = resultRDD.sortByKey(false).repartition(1).collectAsMap();
			
			System.out.println("########## Top Source Station ######## " );
			
	        for (Integer key : result.keySet()) {
	        	System.out.println("Word :::: " + key);
	        	System.out.println("Count :::: " + result.get(key));
	        }	
		
	}
}
