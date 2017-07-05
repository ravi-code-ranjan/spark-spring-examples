package com.ravi.sparkspring.poc.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ravi.sparkspring.poc.job.LineCountFilter;
import com.ravi.sparkspring.poc.job.WordCountJob;

@Component
public class SparkJobMediatorService {

	@Autowired
	WordCountJob wordCountJob;
	
	@Autowired
	LineCountFilter lineCountFilter;
	
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
}
