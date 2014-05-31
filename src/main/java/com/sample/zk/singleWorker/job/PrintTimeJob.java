package com.sample.zk.singleWorker.job;

import java.util.Date;

import org.apache.commons.lang.time.DateFormatUtils;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class PrintTimeJob implements Job{

	public void execute(JobExecutionContext context) throws JobExecutionException {
		System.out.println("+++++++++Print Time Job++++++++++");
		System.out.println(DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss"));
	}

}
