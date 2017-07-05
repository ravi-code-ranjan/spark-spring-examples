package com.ravi.sparkspring.poc.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ravi.sparkspring.poc.job.WordCountJob;
import com.ravi.sparkspring.poc.job.WordCountStreamJob;

@Component
public class SparkJobMediatorService {

	@Autowired
	WordCountJob wordCountJob;
	
	@Autowired
	WordCountStreamJob wordCountStreamJob;
	
	public void testJobWordCount(){
		wordCountJob.count();
	}
	
	public void testJobWordCountStream(){
		try {
			wordCountStreamJob.countInStream();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
