package com.sample.zk.singleWorker;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.BadVersionException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.Transaction;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.Stat;
import org.quartz.CronScheduleBuilder;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.impl.StdSchedulerFactory;

import com.sample.zk.Constants;

/** 
 * 
 * @author qing
 *
 */
public class SingleWorkerManager {

	private static final String GROUP = "single-worker";
	private Scheduler scheduler;
	private ZooKeeper zkClient;
	private String serverType = "_-default-_";//默认serverType类型，我祈祷不会有人估计它和一样
	private static final String REGISTER = "/register";
	private static final String ALIVE = "/alive";
	
	private Watcher dw = new InnerZK();//default watcher;
	
	private boolean isAlive = false;//是否可用
	
	private Object tag = new Object();
	
	private ReentrantLock lock = new ReentrantLock();
	
	private String sid;//当前server标记，可以是IP等，主要用来表达如下描述：某sid上运行**任务；将**任务分配给某sid；
	//真实场景下，可以为IP为192.168.197.2下tomcat运行printNumber任务；
	//任务在哪个server上运行，需要有明确的信息才行，所以sid的设计需要很直观。
	//
	//很多时候，我们都是以ip地址来标记任务被运行的环境地址，不过在有些比较“穷苦的公司”，
	//可能一个物理server下运行多个对等的tomcat实例
	//或许这种方式下，使用ip作为标记，就一些麻烦了。
	
	//已经在本地提交，但尚未提交给zk的任务,直到zk接受任务之后，提交任务者才返回
	//+++++++++++++++++++++++++++++++++++++++++++
	//如何设计任务提交，可能面临一个奇怪的选择，如下是2种队列：
	//LinkedBlockingQueue提供了阻塞与非阻塞两种方式，非阻塞的方式允许任务提交这立即返回，但是此任务此后是否能够被zk
	//正确接受将存在风险，有可能zk的故障，导致此任务无法正常运行。
	//private BlockingQueue<Worker> outgoingWorker  = new LinkedBlockingQueue<Worker>();
	//++++++++++++++++++++++++++++++++++++++++++++
	//同步队列，是一个“单工”队列，如果任务任务被zk正确接受之后，任务提交者才返回，这是一个理想的情况。任务一旦开始被处理，任务提交者就可以返回了。
	//如果任务提交时，刚好zk环境故障，那么此任务将会被重试多次，如果还未能成功，则失败。
	//++++++++++++++++++++++++
	//无论如何，你都需要做出一个选择，我选择了最直观的答案：SynchronousQueue + 同步
	//++++++++++++++++++++++++
	private SynchronousQueue<Worker> outgoingWorker = new SynchronousQueue<Worker>();
	
	//当前serverType下所有的任务
	private Map<String,Worker> allWorkers = new HashMap<String,Worker>();
	
	//当前实例上所运行的任务，它是allWorkers的子集
	private Map<String,Worker> selfWorkers = new HashMap<String,Worker>();
	
	//用来间歇性的与zk进行同步，用来检测job的冲突或者新job的分配
	private Thread syncThread;
	//用于向zk提交任务数据的线程，将和SynchronousQueue协同工作
	private Thread workerThread;
	
	/**
	 * 创建zk实例
	 */
	public SingleWorkerManager(String sid){
		this(sid,null);
		
	}
	
	public SingleWorkerManager(String sid,String sType){
		if(sType != null){
			this.serverType = sType;
		}
		try{
			zkClient = new ZooKeeper(Constants.connectString, 3000, dw,false);
		}catch(Exception e){
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		this.sid = sid;
		syncThread = new Thread(new SyncHandler());
		syncThread.setDaemon(true);
		syncThread.start();
	}
	
	/**
	 * 开启任务调度器
	 */
	public void start(){
		try{
			scheduler = StdSchedulerFactory.getDefaultScheduler();
			scheduler.start();
			workerThread = new Thread(new WorkerHandler());
			workerThread.setDaemon(true);
			workerThread.start();
			isAlive = true;
			synchronized (tag) {
				tag.notifyAll();
			}
			
			//首次同步
			sync();
		}catch(Exception e){
			e.printStackTrace();
			throw new RuntimeException(e);//异常退出
		}
	}
	
	/**
	 * 关闭任务调度器，关闭zookeeper链接
	 * 此后将导致任务被立即取消，singleWorkerManager实例将无法被重用
	 */
	public void close(){
		lock.lock();
		try{
			isAlive = false;
			scheduler.shutdown();
			if (syncThread.isAlive()) {
				syncThread.interrupt();
			}
			if(workerThread.isAlive()){
				workerThread.interrupt();
			}
			if(zkClient != null){
				zkClient.close();
			}
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			lock.unlock();
		}
	}
	
	/**
	 * 取消job，将触发zk服务也“取消”此任务
	 * @param jobName
	 */
	public void unschedule(String jobName){
		try{
			//here，zk
			lock.unlock();
			try{
				String jobPath = "/" + serverType + "/" + jobName;
				Stat stat = zkClient.exists(jobPath, false);
				if(stat != null){
					zkClient.delete(jobPath, stat.getVersion());
				}
			}catch(NoNodeException e){
				//ignore;
			}catch(Exception e){
				e.printStackTrace();
			}
			//有syncHandler来取消本地任务
//			//here,local scheduler
//			//无论如何，本地都要取消
//			TriggerKey key = new TriggerKey(jobName, GROUP);
//			if(scheduler.checkExists(key)){
//				scheduler.unscheduleJob(key);
//			}
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	/**
	 * 提交任务，如果提交失败，将抛出异常
	 * @param jobClass
	 * @param cronExpression
	 * @return true任务提交成功，false任务提交失败
	 */
	public boolean schedule(Class<? extends Job> jobClass,String cronExpression){
		if(!isAlive){
			throw new IllegalStateException("worker has been closed!");
		}
		try{
			Worker worker = this.build(jobClass, cronExpression);
			return outgoingWorker.offer(worker,15,TimeUnit.SECONDS);//waiting here,最多15妙
		}catch(Exception e){
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
	
	private Worker build(Class<? extends Job> jobClass,String cronExpression){
		String name = jobClass.getName();//全路径name
		JobDetail job = JobBuilder.newJob(jobClass).withIdentity(name,GROUP).build();
		CronScheduleBuilder sb = CronScheduleBuilder.cronSchedule(cronExpression);//每两秒执行一次："*/2 * * * * ?"
		Trigger trigger = TriggerBuilder.newTrigger().withIdentity(name, GROUP).withSchedule(sb).build();
		return new Worker(job, trigger,cronExpression);
	}
	
	
	///////////////////////////////////////////////////////inner worker//////////////////////////////
	
	/**
	 * 当前实例的zkClient是否链接正常，scheduler是否处于可用状态
	 * @return
	 */
	private boolean isReady(){
		if(!isAlive){
			return false;
		}
		if(scheduler == null || zkClient == null){
			return false;
		}
		try{
			if(scheduler.isShutdown() || !scheduler.isStarted()){
				return false;
			}
		}catch(Exception e){
			e.printStackTrace();
			return false;
		}
		if(zkClient.getState().isConnected()){
			return true;
		}
		return false;
	}
	
	/**
	 * 同步selfWorkers列表，和zk环境中的列表进行比较，查看是否有任务冲突
	 */
	private void syncSelfWorker(){
		lock.lock();
		try{
			if(!isReady()){
				throw new RuntimeException("Scheduler error..");//以异常的方式中断
			}
			//首先检测自己持有的任务列表，是否和zk一致，首次同步，selfWorkers肯定是空，需要sync后续去做调度。
			for(String job : selfWorkers.keySet()){
				String jobPath = "/" + serverType + "/" + job;
				//如果此任务已经被远程取消,则取消本地job执行
				//所有的实例都会做同样的事情，一定会把那些“取消的任务”取消
				if(zkClient.exists(jobPath, false) == null){
					allWorkers.remove(job);
					Worker cw = selfWorkers.remove(job);
					if(cw != null){
						if(scheduler.checkExists(cw.getJob().getKey())){
							scheduler.unscheduleJob(cw.getTrigger().getKey());
						}
					}
					continue;
				}
				String alive = "/" + serverType + "/" + job + ALIVE;
				//查看是否有子节点冲突，比如一个job被多个server运行
				List<String> alives = zkClient.getChildren(alive, false);
				if(alives == null || alives.isEmpty()){
					//如果此任务尚未分配，则交付给workerHandler
					continue;
				}
				if(alives.size() == 1){
					String holder = alives.get(0);
					//如果已分配且接管者是自己，更新时间
					if(holder.equalsIgnoreCase(sid)){
						byte[] data = String.valueOf(System.currentTimeMillis()).getBytes();
						zkClient.setData(alive + "/" + sid, data, -1);//ignore version
						continue;//如果是自己
					}
				}
				//对于其他情况，当前sid只能让步（有可能会存在所有的sid都让步，导致任务在极短时间内无法运行，
				//后台“补救”线程会做工作）
				if(zkClient.exists(alive + "/" + sid, false) != null){
					try{
						zkClient.delete(alive + "/" + sid, -1);
						scheduler.unscheduleJob(new TriggerKey(job, GROUP));
						selfWorkers.remove(job);
					}catch(NoNodeException e){
						//ignore:
					}catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			lock.unlock();
		}
	}
	

	/**
	 * 同步任务信息，将当前实例中scheduler运行的任务和zk进行比较，进行冲突检测。
	 * 1) 检测自己正在运行的任务，是否和zk中心中分配给自己的任务列表一致。
	 * 2) 获得当前serverType下所有的任务列表
	 * 
	 */
	private void sync(){
		lock.lock();
		try{
			if(!isReady()){
				throw new RuntimeException("Scheduler error..");
			}
			//检测一级节点
			Stat tstat = zkClient.exists("/" + serverType,false);
			if(tstat == null){
				try{
					zkClient.create("/" + serverType, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				}catch(NodeExistsException e){
					//ignore
				}
			}
			//+++++++++++++++++++
			syncSelfWorker();
			//+++++++++++++++++++
			
			//获得所有任务列表
			List<String> allJobs = zkClient.getChildren("/" + serverType, false);
			if(allJobs == null){
				throw new RuntimeException("NO jobs, error..");//以异常的方式，终端方法调用，没有别的意思。
			}
			allWorkers.clear();//reload all
			for(String job : allJobs){
				try{
					//job为类的全名，节点下挂载的数据为cronException
					byte[] data = zkClient.getData("/" + serverType + "/" + job, false, null);
					if(data == null || data.length == 0){
						continue;
					}
					
					//简单考虑吧，不过作为一名合格的程序员，此处可能需要太多的校验。
					Class<? extends Job> jobClass = (Class<? extends Job>)ClassLoader.getSystemClassLoader().loadClass(job);
					Worker worker = build(jobClass, new String(data));
					allWorkers.put(job,worker);
					//自己检测到任务后，注册自己
					String registerPath = "/" + serverType + "/" + job + REGISTER + "/" + sid;
					//如果不存在
					if(zkClient.exists(registerPath, false) == null){
						try{
							zkClient.create(registerPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
						}catch(NodeExistsException ex){
							//ignore;如果自己已经注册过，则忽略
						}
					}
					//检测此worker是否为自己所持有
					String alivePath = "/" + serverType + "/" + job + ALIVE +"/" + sid;
					//如果此任务不属于自己运行，则继续
					if(zkClient.exists(alivePath, false) == null){
						continue;
					}
					//如果属于自己运行，则开启任务，本地是否开启任务，完全取决于zk的数据状态
					try{
						boolean exists = scheduler.checkExists(worker.getJob().getKey());
						if(!exists){
							//如果尚未在当前实例中调度，则立即调度
							scheduler.scheduleJob(worker.getJob(),worker.getTrigger());
							selfWorkers.put(job,worker);
						}
					}catch(Exception e){
						e.printStackTrace();
						zkClient.delete(alivePath, -1);//ignore version;
						//再次校验
						selfWorkers.remove(job);
					}
				}catch(ClassNotFoundException e){
					e.printStackTrace();
					throw new RuntimeException(e);
				}
			}
			
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			lock.unlock();
		}
	}
	
	
	class InnerZK implements Watcher {

		public void process(WatchedEvent event) {
			// 如果是“数据变更”事件
			if (event.getType() != EventType.None) {
				//processExt(event);
				return;
			}
			
			// 如果是链接状态迁移
			// 参见keeperState
			switch (event.getState()) {
			case SyncConnected:
				System.out.println("Connected...");
				// 链接状态迁移时，检测worker信息
				sync();
				break;
			case Expired:
				System.out.println("Expired...");
				break;
			// session过期
			case Disconnected:
				// 链接断开，或session迁移
				System.out.println("Connecting....");
				break;
			case AuthFailed:
				close();
				throw new RuntimeException("ZK Connection auth failed...");
			default:
				break;
			}
		}
		
	}
	
	/**
	 * 分配任务，在所有的worker信息都同步结束后，然后在逐个检测任务状态，对于没有
	 * 被执行的新任务，或者已经失去托管的任务，交付给其他sid。
	 * 
	 * 任务分配，没有采取“严格均衡”的方式，我们使用了一个随即方式。
	 */
	private void scheduler(){
		lock.lock();
		for(String job : allWorkers.keySet()){
			try{
				//如果没有，则创建一个持久节点，挂载数据为，系统时间戳，你可以为此节点加上ACL控制，但会带来复杂度
				//这里可以创建为临时节点，那么你需要对此节点注册watch，当watch触发时（比如其他sid的session失效等）做job的接管
				//考虑到如果大量的job，大量的watch，在网络复杂的情况下，再加上对zk的并发操作，数据一致性是个问题。
				//此处，我们采取挂载“时间戳”的方式，在SyncHandler线程中，间歇性的去检测，惰性的非实时的分配和协调任务
				//此处就要求，你的应用服务器的时间，应该几乎非常一致，如果你无法做到，请在此处增加一个操作分支，从一个统一的地方获得时间：比如DB中等
				String alivePath = "/" + serverType + "/" + job + ALIVE;
				List<String> children = zkClient.getChildren(alivePath, false);//如果节点不存在，则在下一次sync时被补救
				if(children == null || children.isEmpty()){
					//此job尚未分配
					String registerPath = "/" + serverType + "/" + job + REGISTER;
					List<String> rc = zkClient.getChildren(registerPath, false);
					//等待下一次sync时准备节点数据
					if(rc == null || rc.isEmpty()){
						continue;
					}
					Collections.shuffle(rc);//打乱顺序，随即，取出第一个，其实你可以有很多更好的手段来实现“任务均衡”，此处仅为参考
					String tsid = rc.get(0);
					try{
						byte[] data = String.valueOf(System.currentTimeMillis()).getBytes();
						zkClient.create(alivePath + "/" + tsid, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
						//tsid对应的syncHandler此后将会检测并补救。此处只是分配给他。
						//如果tsid也是失去托管的，那么下一次sync检测将会发现并移除，此处不再做多余的校验；
						//在极端情况下，比如你的“任务托管过期时间”过短，或者你的系统发布过程很长，但是所有的任务都失去托管
						//那么最终将会有一台机器接管大部分job，如果job个数很多，将会出现“雪崩效应”;
						//如果你不能容忍这些事情的发生，请在此处增加有效的barrier操作（如果接管任务个数达到一定个数，将接受但不执行任务）
						//或者refuse操作（既不接管也不执行任务）。
						System.out.println("Job switch,SID:" + tsid + ",JOB :" + job);
					}catch(NodeExistsException e){
						//ignore;
					}
					continue;
				}
				//如果job已经被其他sid接管，那么检测接管者，是否处于活跃,如果存在多个子节点，其实是
				//一种异常情况，此处我们只做校验，冲突有sync解决
				for(String id : children){
					String tpath = alivePath + "/" + id;
					Stat stat = new Stat();
					byte[] data = zkClient.getData(tpath, false,stat);
					long time = Long.valueOf(new String(data));
					long current = System.currentTimeMillis();
					//如果一个任务，它的执行者在2分钟内都没有和zk交互（synSelfWorker方法中会更新time）
					//表明已经过期
					//为了便于测试，此处为15秒
					if(time + 1500 < current){
						try{
							zkClient.delete(tpath, stat.getVersion());
						}catch(BadVersionException e){
							//ignore
						}catch(NoNodeException e){
							//ignore;
						}
					}else{
						System.out.println(id + " :" + job);
					}
				}
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		lock.unlock();
	}
	/**
	 * 任务同步线程，间歇性的检测zk持有的任务和本地任务是否一致
	 * 并负责分配任务
	 * @author qing
	 *
	 */
	class SyncHandler implements Runnable {

		public void run() {
			try {
				int i = 0;
				int l = 10;
				while (true) {
					synchronized (tag) {
						try{
							while(!scheduler.isStarted()){
								tag.wait();
							}
						}catch(Exception e){
							//
						}
					}
					System.out.println("Sync handler,running...tid: " + Thread.currentThread().getId());
					if (zkClient == null || (zkClient.getState() == States.NOT_CONNECTED || zkClient.getState() == States.CLOSED)) {
						lock.lock();
						try {
							// 回话重建等异常行为
							zkClient = new ZooKeeper(Constants.connectString, 3000, dw, false);
							System.out.println("Reconnected success!...");
						} catch (Exception e) {
							e.printStackTrace();
							i++;
							Thread.sleep(3000 + i * l);// 在zk环境异常情况下，每3秒重试一次
						} finally {
							lock.unlock();
						}
						continue;
					}
					if (zkClient.getState().isConnected()) {
						sync();//同步任务
						scheduler();//任务分配和过期检测
						Thread.sleep(3000);// 如果被“中断”，直接退出
						i = 0;
					}else{
						Thread.sleep(3000);
					}
				}
			} catch (InterruptedException e) {
				System.out.println("SID:" + sid + ",SyncHandler Exit...");
				close();
			}

		}
	}
	
	/**
	 * 调用者提交的任务，将会被同步的方式交付给zk。此线程就是负责从queue中获取调用者
	 * 提交的job，然后依次在zk环境中生成节点数据。
	 * @author qing
	 *
	 */
	class WorkerHandler implements Runnable{
		private Set<Worker> pending = new HashSet<Worker>();
		private int count = 0;//max = 20;
		
		/**
		 * 将worker信息生成zk节点数据
		 * @param worker
		 * @return
		 */
		private boolean register(Worker worker){
			lock.lock();
			//逐级创建其父节点
			String jobName = worker.getJob().getKey().getName();
			try{
				Transaction tx = zkClient.transaction();//使用事务的方式
				String jobPath = "/" + serverType + "/" + jobName;
				if(zkClient.exists(jobPath, false) == null){
					tx.create(jobPath, worker.getCronExpression().getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				}
				String registerPath = "/" + serverType + "/" + jobName+ REGISTER;
				if(zkClient.exists(registerPath, false) == null){
					tx.create(registerPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				}
				String alivePath = "/" + serverType + "/" + jobName+ ALIVE;
				if(zkClient.exists(alivePath, false) == null){
					tx.create(alivePath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				}
				tx.create(registerPath + "/" + sid, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				tx.commit();
			}catch(NodeExistsException e){
				//ignore
			}catch(Exception e){
				e.printStackTrace();
				pending.add(worker);
				//对于异常数据，添加到一个补充操作队列，如果在操作中出现异常，那么将会在
				//补充操作中得到再次校验
			}
			lock.unlock();
			return true;
		}
		
		public void run(){
			try{
				while(true){
					synchronized (tag) {
						try{
							while(!scheduler.isStarted()){
								tag.wait();
							}
						}catch(Exception e){
							//
						}
					}
					System.out.println("Worker handler,running...");
					if(zkClient != null && zkClient.getState().isConnected()){
						System.out.println("Register...");
						Worker worker = outgoingWorker.take();
						register(worker);
						if(!pending.isEmpty()){
							Thread.sleep(500);
							Iterator<Worker> it = pending.iterator();
							while(it.hasNext()){
								boolean isOk = register(it.next());
								if(!isOk){
									count++;
									Thread.sleep(1000);
								}else{
									count = 0;
									it.remove();
								}
								//如果重试20次，仍无法成功，直接抛弃，非常遗憾
								if(count > 20){
									pending.clear();
								}
							}
						}
						
					}else{
						Thread.sleep(1000);
					}
				}
				
			}catch(InterruptedException e){
				System.out.println("SID:" + sid + ",WorkerHandler Exit...");
				close();
			}
		}
	}
	
	/**
	 * 全部删除当前serverType下所有的任务
	 */
	public void clear(){
		lock.lock();
		try{
			if(zkClient != null && zkClient.getState().isConnected()){
				zkClient.delete("/" + serverType, -1);
			}
//			if(scheduler != null && scheduler.isStarted()){
//				for(Worker worker : selfWorkers.values()){
//					scheduler.unscheduleJob(worker.getTrigger().getKey());
//				}
//			}
//			allWorkers.clear();
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			lock.unlock();
		}
	}

}
