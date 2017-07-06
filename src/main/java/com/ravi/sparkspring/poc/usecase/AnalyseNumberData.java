package com.ravi.sparkspring.poc.usecase;

import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ravi.sparkspring.poc.config.ApplicationConfiguration;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;

@Component
public class AnalyseNumberData {


    @Autowired
    JavaSparkContext javaSparkContext;
    
    @Autowired
    ApplicationConfiguration applicationConfiguration;
    
    public void analyseNumberData(){
    	
    	//https://acadgild.com/blog/spark-sql-use-case-911-emergency-helpline-number-data-analysis/

    	String inputFile1 = applicationConfiguration.getNumberDataAnalysisInput1();
    	
    	String inputFile2 = applicationConfiguration.getNumberDataAnalysisInput2();
    	
    	analyseData(javaSparkContext, inputFile1, inputFile2);
    }

	private static void analyseData(JavaSparkContext javaSparkContext2,
			String inputFile, String inputFile2) {

		SQLContext sqlContext = new SQLContext(javaSparkContext2);

		Dataset<Row> zipCodeDataFrame = sqlContext.read()
				.format("com.databricks.spark.csv")
				.option("inferSchema", "true")
				.option("header", "true")
				.load(inputFile);

		Dataset<Row> emergencyDataFrame = sqlContext.read()
				.format("com.databricks.spark.csv")
				.option("inferSchema", "true")
				.option("header", "true")
				.load(inputFile2);

		System.out.println(" " + zipCodeDataFrame.count() + " " + emergencyDataFrame.count());

		sqlContext.registerDataFrameAsTable(emergencyDataFrame, "emergency");

		sqlContext.registerDataFrameAsTable(zipCodeDataFrame, "zipcode");

		Dataset<Row> joinedDataFrame = sqlContext.sql("select e.title, z.city,z.state from emergency e join zipcode z on e.zip = z.zip");

		probleminCity(joinedDataFrame);
		probleminState(joinedDataFrame);
	}

	private static void probleminCity(Dataset<Row> joinedDataSet) {
		
		JavaPairRDD<String,Integer> rdd = joinedDataSet.toJavaRDD().map(new Function<Row, String>() {
			private static final long serialVersionUID = -6133165404841203143L;

			@Override
			public String call(Row v1) throws Exception {
				return v1.getString(0).split(":")[0] + "-->" + v1.getString(1);
			}
		}).mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String t) throws Exception {
				return new Tuple2<String, Integer>(t, 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});

		Map<String, Integer> result = rdd.sortByKey(true).repartition(1).collectAsMap();
		
		System.out.println("########## Kind of problem in City ######## " );
		
        for (String key : result.keySet()) {
        	System.out.println("Key :::: " + key);
        	System.out.println("Value :::: " + result.get(key));
        }	
	}

	private static void probleminState(Dataset<Row> joinedDataSet) {
		
		JavaPairRDD<String,Integer> rdd = joinedDataSet.toJavaRDD().map(new Function<Row, String>() {
			private static final long serialVersionUID = -6133165404841203143L;

			@Override
			public String call(Row v1) throws Exception {
				return v1.getString(0).split(":")[0] + "-->" + v1.getString(2);
			}
		}).mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String t) throws Exception {
				return new Tuple2<String, Integer>(t, 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});

		Map<String, Integer> result = rdd.sortByKey(true).repartition(1).collectAsMap();
		
		System.out.println("########## Kind of problem in State ######## " );
		
        for (String key : result.keySet()) {
        	System.out.println("Key :::: " + key);
        	System.out.println("Value :::: " + result.get(key));
        }	
}
}
