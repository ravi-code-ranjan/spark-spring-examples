package com.ravi.sparkspring.poc.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ravi.sparkspring.poc.job.LineCountFilter;
import com.ravi.sparkspring.poc.job.WordCountJob;
import com.ravi.sparkspring.poc.usecase.AnalyseDailyShowData;
import com.ravi.sparkspring.poc.usecase.AnalyseTravelData;
import com.ravi.sparkspring.poc.usecase.AnalyseWeatherData;
import com.ravi.sparkspring.poc.usecase.AnalysisCrimeData;
import com.ravi.sparkspring.poc.usecase.AnalysisOlympicsData;

@Component
public class SparkJobMediatorService {

	@Autowired
	WordCountJob wordCountJob;
	
	@Autowired
	LineCountFilter lineCountFilter;
	
	@Autowired
	AnalysisCrimeData analysisCrimeData;
	
	@Autowired
	AnalysisOlympicsData analysisOlympicsData;
	
	@Autowired
	AnalyseDailyShowData dailyShowData;
	
	@Autowired
	AnalyseTravelData analyseTravelData;
	
	@Autowired
	AnalyseWeatherData analyseWeatherData;
	
/*	@Autowired
	WordCountStreamJob wordCountStreamJob;*/
	
	public void testJobWordCount(){
		wordCountJob.count();
	}
	
/*	public void testJobWordCountStream(){
		try {
			wordCountStreamJob.countInStream();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}*/
	
	public void testlineCount(){	
		lineCountFilter.count();
	}
	
	public void testCrimeData(){
		analysisCrimeData.analyse();
	}
	
	public void testOlympicsData(){
		analysisOlympicsData.analyseOlympics();
	}
	
	public void testDailyShowData(){
		dailyShowData.analyseDailyShow();
	}
	
	public void testAnalyseTravelData(){
		analyseTravelData.analyseTravelData();
	}
	
	public void testAnalyseWeatherData(){
		analyseWeatherData.analyseWeatherData();
	}
	
}
