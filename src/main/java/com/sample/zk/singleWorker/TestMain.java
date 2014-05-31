package com.sample.zk.singleWorker;

import com.sample.zk.singleWorker.job.PrintNumberJob;
import com.sample.zk.singleWorker.job.PrintTimeJob;

public class TestMain {

	/**
	 * @param args
	 */
	public static void main(String[] args){
		try{
			//jobSchedule()
			jobSwitch();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	private static void jobSchedule() throws Exception{
		SingleWorkerManager wm1 = new SingleWorkerManager("test1");//sid此处为硬编码，你需要确保实际环境中，每个实例都不相同
		wm1.start();
		wm1.schedule(PrintNumberJob.class, "*/2 * * * * ?");
		System.out.println("////printNumber...");
		wm1.schedule(PrintTimeJob.class, "*/2 * * * * ?");
		System.out.println("////printTime...");
	}
	
	private static void jobSwitch() throws Exception{
		SingleWorkerManager wm1 = new SingleWorkerManager("test1");//sid此处为硬编码，你需要确保实际环境中，每个实例都不相同
		//++++++++++++++++++
		wm1.clear();//测试使用
		//++++++++++++++++++
		wm1.start();
		wm1.schedule(PrintNumberJob.class, "*/2 * * * * ?");
		System.out.println("////printNumber...");
		wm1.schedule(PrintTimeJob.class, "*/2 * * * * ?");
		System.out.println("////printTime...");
		//假如wm失效,我们将会发现任务已经被迁移到wm2.
		wm1.close();
		Thread.sleep(20000);
		SingleWorkerManager wm2 = new SingleWorkerManager("test2");
		wm2.start();
		wm2.schedule(PrintNumberJob.class, "*/2 * * * * ?");
		System.out.println("////printNumber...");
		wm2.schedule(PrintTimeJob.class, "*/2 * * * * ?");
		System.out.println("////printTime...");
	}
}
