package com.ravi.sparkspring.poc.job;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ravi.sparkspring.poc.config.ApplicationConfiguration;

@Component
public class LineCountFilter {

    @Autowired
    JavaSparkContext javaSparkContext;
    
    @Autowired
    ApplicationConfiguration applicationConfiguration;
     
	public void count(){
		
		// Read the source file
		String filePath = applicationConfiguration.getFileWordCountPath();
	    JavaRDD<String> input = javaSparkContext.textFile(filePath);
	    
	    JavaRDD<String> nonEmptyLines = input.filter(filterForFile);

	    long count = nonEmptyLines.count();

	    System.out.println(String.format("Total lines in %s is %d",filePath,count));
	}
	
    static Function<String, Boolean> filterForFile = new Function<String, Boolean>() {

		private static final long serialVersionUID = 1L;

		@Override
		public Boolean call(String s) throws Exception {
	        if(s == null || s.trim().length() < 1) {
		        return false;
		    }
		        return true;
        	
		}
     };
}
