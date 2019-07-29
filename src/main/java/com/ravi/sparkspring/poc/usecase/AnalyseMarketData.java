package com.ravi.sparkspring.poc.usecase;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ravi.sparkspring.poc.config.ApplicationConfiguration;

import scala.Tuple2; 

@Component
public class AnalyseMarketData {

    @Autowired
    JavaSparkContext javaSparkContext;
    
    @Autowired
    ApplicationConfiguration applicationConfiguration;

    public void count() {

        String filePath = applicationConfiguration.getFileWordCountPath();
                
        /**
         * Approach 1 with lambdas
         */

        JavaRDD<String> textFile = javaSparkContext.textFile(filePath);
        Function<String, Boolean> filter = k -> ( k.contains(".edu") == true);
        
        JavaRDD<String> allCount = textFile.filter(filter);
        
        System.out.println("going to save file");
        allCount.coalesce(1,true).saveAsTextFile("exact path");
        
        
        /**
         * Approach 1 end
         */

 
        /**
         * Approach 2 with filter and split
         */
        /** JavaPairRDD<String, String> rddResult = javaSparkContext.textFile(filePath)
				.filter(domainFilter)
				.mapToPair(split)
				.reduceByKey(reduce);
		
        rddResult.coalesce(1, true).saveAsTextFile("exact path");

        System.out.println("File saved"); **/
    }  
	
    static Function<String, Boolean> domainFilter = new Function<String, Boolean>() {
		private static final long serialVersionUID = 1L;

		@Override
		public Boolean call(String s1) throws Exception {
			if(s1.endsWith(".edu") == true) {
				return true;
			}
			else
				return false;
		}
	};
	
    static PairFunction<String, String, String> split = new PairFunction<String, String, String>() {
		
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<String, String> call(String t) throws Exception {
			String splits[] = t.split(":");
			return new Tuple2<String, String>(splits[1],splits[0]);
		}
	};
	
    static Function2<String, String, String> reduce = new Function2<String, String, String>() {
		private static final long serialVersionUID = 1L;

		@Override
		public String call(String v1, String v2) throws Exception {
			return v2;
		}
	};
}
