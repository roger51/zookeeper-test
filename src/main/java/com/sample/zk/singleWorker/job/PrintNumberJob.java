package com.sample.zk.singleWorker.job;

import java.util.Random;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 * 有个小问题需要阐明，在非spring环境中，Trigger每触发一次，都会创建一个心的job实例，因此context中put的数据，将会在
 * job执行完成后被clear，当然你在job类中定义的成员变量值每次执行都会重置。
 * 
 * spring环境中，可以确保一种类型的job只会有一个bean实例存在而不是每次执行都创建。
 * @author qing
 *
 */
public class PrintNumberJob implements Job{

	public void execute(JobExecutionContext context) throws JobExecutionException {
		Random r = new Random();
		System.out.println("+++++++++Print Number Job++++++++++");
		System.out.println(r.nextInt(100));
	}

}
