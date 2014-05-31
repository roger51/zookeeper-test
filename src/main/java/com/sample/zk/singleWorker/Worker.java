package com.sample.zk.singleWorker;

import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Trigger;

public class Worker {

	private JobDetail job;
	private Trigger trigger;
	
	private String cronExpression;//荣誉保留
	
	public Worker(JobDetail job, Trigger trigger,String cronExpression) {
		this.job = job;
		this.trigger = trigger;
		this.cronExpression = cronExpression;
	}

	public JobDetail getJob() {
		return job;
	}

	public void setJob(JobDetail job) {
		this.job = job;
	}

	public Trigger getTrigger() {
		return trigger;
	}

	public void setTrigger(Trigger trigger) {
		this.trigger = trigger;
	}
	
	public String getCronExpression() {
		return cronExpression;
	}

	@Override
	public boolean equals(Object obj){
		if(obj == null){
			return false;
		}
		if(obj instanceof Worker){
			JobKey jk = ((Worker) obj).getJob().getKey();
			return this.getJob().getKey().equals(jk);
		}
		return false;
	}
	

}
