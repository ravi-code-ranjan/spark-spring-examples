package com.ravi.sparkspring.poc.usecase;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
public class AnalyseUberData {
    
    @Autowired
    JavaSparkContext javaSparkContext;
    
    @Autowired
    ApplicationConfiguration applicationConfiguration;
    
    
    public void analyseUberData(){
    	
    	//https://acadgild.com/blog/spark-use-case-uber-data-analysis/

    	String inputFile = applicationConfiguration.getUberData();
    	
    	analyseData(javaSparkContext, inputFile);
    }

	private static  void analyseData(JavaSparkContext javaSparkContext2,
			String inputFile) {

		String days[] = new String[] { "Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat" };
		SimpleDateFormat sf = new SimpleDateFormat("MM/dd/yyyy");
		
		String line = javaSparkContext2.textFile(inputFile).first();
		
		List<Tuple2<String, Integer>> result = javaSparkContext2.textFile(inputFile)
			
			.filter(x -> !x.equals(line))
			
			.map((x) -> x.split(","))
			
			.map((x) -> new Tuple3<>(x[0], days[sf.parse(x[1]).getDay()], Integer.parseInt(x[3])))
			
			.map(new Function<Tuple3<String, String, Integer>, Map<String, Integer>>() {
				
				private static final long serialVersionUID = 1L;

				@Override
				public Map<String, Integer> call(Tuple3<String, String, Integer> arg0) throws Exception {
					Map<String, Integer> map = new HashMap<String, Integer>();
					map.put(arg0._1() + "" + arg0._2(), arg0._3());
					return map;
				}
			}).mapToPair(new PairFunction<Map<String, Integer>, String, Integer>() {
				
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<String, Integer> call(Map<String, Integer> arg0) throws Exception {
					String key = new ArrayList<String>(arg0.keySet()).get(0);
					return new Tuple2<String, Integer>(key, arg0.get(key));
				}
			}).reduceByKey(new Function2<Integer, Integer, Integer>() {
				
				private static final long serialVersionUID = 1L;

				@Override
				public Integer call(Integer arg0, Integer arg1) throws Exception {
					return arg0 + arg1;
				}
			})
			.collect();
		
		for(Tuple2<String, Integer> tuple : result){
			System.out.println(tuple._1());
			System.out.println(tuple._2());
			
		}
		
		//System.out.println(result);
		
	}
}
