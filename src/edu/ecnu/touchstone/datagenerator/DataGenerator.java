package edu.ecnu.touchstone.datagenerator;

import edu.ecnu.touchstone.controller.JoinInfoMerger;
import edu.ecnu.touchstone.outerjoin.ReadOutJoinTable;
import edu.ecnu.touchstone.outerjoin.WriteOutJoinTable;
import edu.ecnu.touchstone.pretreatment.TableGeneTemplate;
import edu.ecnu.touchstone.run.Configurations;
import edu.ecnu.touchstone.run.Statistic;
import edu.ecnu.touchstone.run.Touchstone;
import edu.ecnu.touchstone.threadpool.TouchStoneThreadPool;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;

// in practice, multiple data generators are deployed in general
// main functions: generate data, maintain join information of the primary key
public class DataGenerator implements Runnable {

	private Logger logger = null;

	// running configurations
	private Configurations configurations = null;
	
	// the ID of the data generator
	private int generatorId;

	public DataGenerator(Configurations configurations, int generatorId) {
		super();
		this.configurations = configurations;
		this.generatorId = generatorId;
		logger = Logger.getLogger(Touchstone.class);
	}

	// each data generation thread has a blocking queue for transmitting 'TableGeneTemplate'
	private static List<ArrayBlockingQueue<TableGeneTemplate>> templateQueues = null;

	// the client linked with the server of the controller
	// it is used for sending 'pkJoinInfo'
	private DataGeneratorClient client = null;


	// store all 'pkJoinInfo's maintained by data generation threads
	// firstly, merge locally, then send to controller
	private static List<Map<Integer, ArrayList<long[]>>> pkJoinInfoList = null;

	private static Map<Integer,Long> pkJoinInfoFileTotalSize =null;

	// control the time point of merging the join information of the primary key (pkJoinInfoList)
	private static CountDownLatch countDownLatch = null;

	@Override
	public void run() {

		pkJoinInfoList = new ArrayList<Map<Integer, ArrayList<long[]>>>();
		pkJoinInfoFileTotalSize =new HashMap<>();
		int threadNum = configurations.getDataGeneratorThreadNums().get(generatorId);
		countDownLatch = new CountDownLatch(threadNum);

		setUpDataGenerationThreads();
		setUpNetworkThreads();
		setUpFileThread();

		while (true) {
			try {
				countDownLatch.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			//如果唤醒时发现，没有joinInfo，则关闭所有线程并退出
			if(pkJoinInfoList.size()==0){
				TouchStoneThreadPool.closeThreadPool();
				try {
					FileUtils.cleanDirectory(new File(configurations.getJoinTableOutputPath()));
				} catch (IOException e) {
					logger.error(e);
				}
				client.send(new HashMap<>());
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				System.exit(0);
			}
			logger.info("\n\tStart merging 'pkJoinInfoList' ...");
			if(pkJoinInfoFileTotalSize.size()==0){
				client.send(JoinInfoMerger.merge(pkJoinInfoList, configurations.getPkvsMaxSize()));
			}else {
				client.send(JoinInfoMerger.merge(pkJoinInfoList, pkJoinInfoFileTotalSize));
			}
			logger.info("\n\tMerge end!");
			logger.info("\n\tThe fkMissCount: " + Statistic.fkMissCount);

			pkJoinInfoList.clear();
			countDownLatch = new CountDownLatch(threadNum);
		}
	}

	private void setUpFileThread(){
        WriteOutJoinTable.setMaxSizeofJoinInfoInMemory(configurations.getMaxSizeofJoinInfoInMemory());
        WriteOutJoinTable.setMaxNumInMemory(configurations.getMaxNumofJoinInfoInMemory());
		ReadOutJoinTable.setMaxNumOfJoinInfoInMemory(configurations.getMaxNumofJoinInfoInMemory());
		ReadOutJoinTable.setMinSizeofJoinStatus(configurations.getMinSizeofJoinInfoStatus());
    }


	private void setUpDataGenerationThreads() {
		// 'localThreadNum' is the number of threads on this node
		int localThreadNum = configurations.getDataGeneratorThreadNums().get(generatorId);
		// 'allThreadNum' is the number of threads on the all nodes
		// 'count' is the number of threads on the previous (generatorId - 1) nodes
		int allThreadNum = 0, count = 0;
		for (int i = 0; i < configurations.getDataGeneratorThreadNums().size(); i++) {
			allThreadNum += configurations.getDataGeneratorThreadNums().get(i);
			if (i == generatorId - 1) {
				count = allThreadNum;
			}
		}
		
		// set up all data generation threads
		templateQueues = new ArrayList<ArrayBlockingQueue<TableGeneTemplate>>();
		for (int i = 0; i < localThreadNum; i++) {
			templateQueues.add(new ArrayBlockingQueue<TableGeneTemplate>(1));
			int threadId = count + i;
			TouchStoneThreadPool.getThreadPoolExecutor().submit(
					new DataGenerationThread(templateQueues.get(i), threadId, allThreadNum,
					configurations.getDataOutputPath(),configurations.getJoinTableOutputPath()));
		}
		logger.info("\n\tAll data generation threads startup successful!");
	}

	// set up the server and client of the data generator
	// server: receive the data generation task (TableGeneTemplate)
	// client: send the maintained join information of the primary key
	private void setUpNetworkThreads() {
		int serverPort = configurations.getDataGeneratorPorts().get(generatorId);
		String controllerIp = configurations.getControllerIp();
		int controllerPort = configurations.getControllerPort();
		TouchStoneThreadPool.getThreadPoolExecutor().submit(new DataGeneratorServer(serverPort));
		client = new DataGeneratorClient(controllerIp, controllerPort);
		TouchStoneThreadPool.getThreadPoolExecutor().submit(client);
	}
	
	// it's called by 'DataGeneratorServerHandler' when receiving a data generation task (template)
	// 'synchronized' is no needed because 'template' must have been sent one by one (last one has been processed)
	public static void addTemplate(TableGeneTemplate template) {
		// to avoid the interference among data generation threads, we assign a deep copy of 'template' to each thread
		// note: 'fksJoinInfo' only has a shallow copy
		try {
			for (int i = 0; i < templateQueues.size(); i++) {
				templateQueues.get(i).put(new TableGeneTemplate(template));
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static void makeDataGeneratorBeginToExist(){
		long count=countDownLatch.getCount();
		for (long i = 0; i < count; i++) {
			countDownLatch.countDown();
		}
	}

	// collect all 'pkJoinInfo's  maintained by data generation threads
	public static synchronized void addPkJoinInfo(Map<Integer, ArrayList<long[]>> pkJoinInfo) {
		pkJoinInfoList.add(pkJoinInfo);
		countDownLatch.countDown();
	}

	static synchronized void addOuterJoinInfo(Map<Integer, ArrayList<long[]>> pkJoinInfo,
											  Map<Integer, Long> pkJoinInfoFileSize){
		pkJoinInfoList.add(pkJoinInfo);
		for (Integer status : pkJoinInfoFileSize.keySet()) {
			if(!pkJoinInfoFileTotalSize.containsKey(status)){
				pkJoinInfoFileTotalSize.put(status,pkJoinInfoFileSize.get(status));
			}else {
				pkJoinInfoFileTotalSize.put(status,pkJoinInfoFileSize.get(status)+pkJoinInfoFileTotalSize.get(status));
			}
		}
		countDownLatch.countDown();
	}
	
	// test
	public static void main(String[] args) {
		PropertyConfigurator.configure(".//test//lib//log4j.properties");
		Configurations configurations = new Configurations(".//test//touchstone2.conf");
		// in a JVM, you can only have one data generator
		// because there are some static attributes in class 'DataGenerator'
		for (int i = 0; i < configurations.getDataGeneratorIps().size(); i++) {
			TouchStoneThreadPool.getThreadPoolExecutor().submit(new DataGenerator(configurations, i));
		}
	}
}

class DataGenerationThread implements Runnable {

	private BlockingQueue<TableGeneTemplate> templateQueue = null;
	private int threadId;
	// the number of all threads in all data generators
	private int threadNum;
	private String dataOutputPath = null;
	private String joinTableOutputPath = null;
	private Map<String, Integer> eachTableLeftJoinTag =new HashMap<>();

	public DataGenerationThread(BlockingQueue<TableGeneTemplate> templateQueue, int threadId,
			int threadNum, String dataOutputPath, String joinTableOutPath) {
		super();
		this.templateQueue = templateQueue;
		this.threadId = threadId;
		this.threadNum = threadNum;
		this.dataOutputPath = dataOutputPath;
		this.joinTableOutputPath=joinTableOutPath;
	}

	@Override
	public void run() {
		try {
			StringBuilder sb = new StringBuilder();
			while (true) {
				TableGeneTemplate template = templateQueue.take();
				long tableSize = template.getTableSize();
				File outputFile = new File(dataOutputPath + "//" + template.getTableName() + "_" + threadId + ".txt");
				BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new
						FileOutputStream(outputFile), "UTF-8"));
				if(template.hasLeftOuterJoin()){
					template.setWriteOutJoinTable(joinTableOutputPath+ "//" +
							template.getTableName() + "_" + threadId + "_");
				}
				if(template.hasLeftOuterJoinFk()){
					template.setReadOutJoinTable(joinTableOutputPath+"//",threadId,eachTableLeftJoinTag);
				}
				for (long uniqueNum = threadId; uniqueNum < tableSize; uniqueNum += threadNum) {
					String[] tuple = template.geneTuple(uniqueNum);
					for (int i = 0; i < tuple.length - 1; i++) {
						sb.append(tuple[i]);
						sb.append(",");
					}
					sb.append(tuple[tuple.length - 1]);
					sb.append("\n");
					bw.write(sb.toString());
					sb.setLength(0);
				}
				if (template.hasLeftOuterJoin() && template.getPkJoinInfoFileSize()!=null) {
					DataGenerator.addOuterJoinInfo(template.getPkJoinInfo(), template.getPkJoinInfoFileSize());
					eachTableLeftJoinTag.put(template.getTableName(), 3 * template.getLeftOuterJoinTag());
				} else {
					DataGenerator.addPkJoinInfo(template.getPkJoinInfo());
				}
				bw.close();
				template.stopAllFileThread();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

