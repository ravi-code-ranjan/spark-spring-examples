package com.ravi.sparkspring.poc.usecase;

import java.util.function.ToDoubleFunction;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ravi.sparkspring.poc.config.ApplicationConfiguration;

@Component
public class AnalysisCrimeData {

    @Autowired
    JavaSparkContext javaSparkContext;
    
    @Autowired
    ApplicationConfiguration applicationConfiguration;
    
    
    public void analyse(){
    	
    	//https://acadgild.com/blog/analyzing-new-york-crime-data-using-sparksql/
    	
    	SQLContext sqlContext = new SQLContext(javaSparkContext);

    	Dataset<Row> crimeDataFrame = sqlContext.read()
    			.format("com.databricks.spark.csv")
    			.option("inferSchema", "true")
    			.option("header", "false")
    			.load(applicationConfiguration.getCrimeAnalysisInput());
    	
    	crimeDataFrame.registerTempTable("crimes");
    	
    	Dataset<Row> crimesUnderFBI = sqlContext.sql("select _c14,count(_c14) from crimes group by _c14");
    	crimesUnderFBI.show();

    	crimesUnderFBI = sqlContext.sql("select count(*) as count from crimes where _c5 ='NARCOTICS' and _c17 = 2015 ");
    	crimesUnderFBI.show();
    	
    	crimesUnderFBI = sqlContext.sql("select _c11 ,count(*) as count from crimes where _c5 ='THEFT' and _c8 = 'true' group by _c11 ");
    	crimesUnderFBI.show();
    	
    	double[] data = crimeDataFrame.select("_c0").javaRDD()
    			.map(mapper).collect().stream()
    			.mapToDouble(toDoubleConert).toArray();
    	
    	DescriptiveStatistics DescriptiveStatistics = new DescriptiveStatistics(data);
    	
    	double meanQ1 = DescriptiveStatistics.getPercentile(25);
    	double mean = DescriptiveStatistics.getPercentile(50);
    	double meanQ3 = DescriptiveStatistics.getPercentile(75);
    	
    	double meanQ2 = meanQ3 - meanQ1;
    	
    	System.out.println(meanQ1+" "+meanQ2+" "+meanQ3+" "+mean);
    	System.out.println();
    }
    
    
    static Function<Row, Double> mapper = new Function<Row, Double>() {

		private static final long serialVersionUID = 1L;

		@Override
		public Double call(Row v1) throws Exception {
			return ((double)v1.getInt(0));
		}
	};
   
	
   static ToDoubleFunction<Double> toDoubleConert = new ToDoubleFunction<Double>() {

		@Override
		public double applyAsDouble(Double value) {
			return value;
		}
	};
	
}
