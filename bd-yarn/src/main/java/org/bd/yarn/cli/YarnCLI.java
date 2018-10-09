package org.bd.yarn.cli;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.bd.yarn.utils.AppContext;
import org.bd.yarn.utils.YarnAppHandle;
import org.bd.yarn.utils.YarnAppInfo;

public class YarnCLI {

	private Options opts;
	private AppContext appContext;
	
	public YarnCLI(){
		initOptions();
	}

	private void initOptions() {
		opts = new Options();
		opts.addOption("appname", true, "Application Name. Default value - Hello_WPK");
		opts.addOption("apptype", true, "Application Type. Default value - YARN");
		opts.addOption("priority", true, "Application Priority. Default 0");
		opts.addOption("queue", true, "RM Queue in which this application is to be submitted");
		opts.addOption("master_memory", true, "Amount of memory in MB to be requested to run the application master");
		opts.addOption("master_vcores", true, "Amount of virtual cores to be requested to run the application master");
		opts.addOption("jar", true, "Jar file containing the application master");
		opts.addOption("classname", true, ".......");
		opts.addOption("container_memory", true, "Amount of memory in MB to be requested to run the HelloYarn");
		opts.addOption("container_vcores", true, "Amount of virtual cores to be requested to run the HelloYarn");
		opts.addOption("num_containers", true, "No. of containers on which the HelloYarn needs to be executed");
		opts.addOption("help", false, "Print usage");
	}

	private void printUsage() {
		new HelpFormatter().printHelp("Client", opts);
	}

	public boolean init(String[] args) throws ParseException {

		CommandLine cliParser = new GnuParser().parse(opts, args);
		appContext = new AppContext();

		if (args.length == 0) {
			throw new IllegalArgumentException("No args specified for client to initialize");
		}

		if (cliParser.hasOption("help")) {
			printUsage();
			return false;
		}

		if(cliParser.hasOption("appname")){
			appContext.setAppName(cliParser.getOptionValue("appname"));
		}
		if(cliParser.hasOption("apptype")){
			appContext.setAppType(cliParser.getOptionValue("apptype"));
		}
		if(cliParser.hasOption("master_memory")){
			int num = Integer.parseInt(cliParser.getOptionValue("master_memory"));
			appContext.setAmMemory(num);
			if(num < 0) {
				throw new IllegalArgumentException("指定应用内存无效,master_memory=" + num);
			}
		}
		if(cliParser.hasOption("master_vcores")){
			int num = Integer.parseInt(cliParser.getOptionValue("master_vcores"));
			appContext.setAmVCores(num);
			if(num < 0) {
				throw new IllegalArgumentException("指定应用虚拟CPU无效, master_vcores=" + num);
			}
		}
		if(cliParser.hasOption("priority")){
			appContext.setAmPriority(Integer.parseInt(cliParser.getOptionValue("priority")));
		}
		if(cliParser.hasOption("queue")){
			appContext.setAmQueue(cliParser.getOptionValue("queue"));
		}
		if(cliParser.hasOption("jar")){
			appContext.setAppJarPath(cliParser.getOptionValue("jar"));
		}else{
			throw new IllegalArgumentException("请指定应用的jar文件！");
		}
		if(cliParser.hasOption("classname")){
			appContext.setAppJarPath(cliParser.getOptionValue("classname"));
		}else{
			throw new IllegalArgumentException("请指定应用的类名！");
		}
		if(cliParser.hasOption("container_memory")){
			int num = Integer.parseInt(cliParser.getOptionValue("container_memory"));
			appContext.setContainerMemory(num);
			if (num < 0){
				throw new IllegalArgumentException("指定容器内存无效,container_memory=" + num);
			}
		}
		if(cliParser.hasOption("container_vcores")){
			int num = Integer.parseInt(cliParser.getOptionValue("container_vcores"));
			appContext.setContainerVCores(num);
			if(num < 0){
				throw new IllegalArgumentException("指定容器虚拟CPU无效,container_vcores=" + num);
			}
		}
		if(cliParser.hasOption("num_containers")){
			int num = Integer.parseInt(cliParser.getOptionValue("num_containers"));
			appContext.setNumContainers(num);
			if(num < 1){
				throw new IllegalArgumentException("指定容器数量无效,num_containers=" + num);
			}
		}
		if(cliParser.hasOption("num_containers")){
			
		}

		return true;
	}

	public boolean run() throws IOException, YarnException {
		YarnAppHandle yarnAppHandle = new YarnAppHandle();
		ApplicationId appId = yarnAppHandle.submitApp(appContext);
		boolean flag = monitorApplication(appId);
		return flag;
	}

	private boolean monitorApplication(ApplicationId appId) throws YarnException, IOException {
		while (true) {
			try {
				//每秒检查一次状态
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			YarnAppInfo yarnAppInfo = new YarnAppInfo();
			ApplicationReport report = yarnAppInfo.getAppById(appId);
			YarnApplicationState state = report.getYarnApplicationState();
			FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
			if(YarnApplicationState.FINISHED == state) {
				if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
					System.out.print("应用执行完成且成功！应用ID为："+appId.getId());
					return true;
				} else {
					System.out.print("应用执行完成且失败！应用ID为："+appId.getId());
					return false;
				}
			}else if(YarnApplicationState.KILLED == state || YarnApplicationState.FAILED == state) {
				System.out.print("应用执行未完成且失败！应用ID为："+appId.getId());
				return false;
			}else{
				float progress = report.getProgress();
				System.out.println("应用执行进度---------------------------------------"+progress);
			}
		}
	}

	public static void main(String[] args) {
		boolean result = false;
		try {
			YarnCLI rarnCLI = new YarnCLI();
			try {
				boolean doRun = rarnCLI.init(args);
				if (!doRun) {
					System.exit(0);
				}
			} catch (IllegalArgumentException e) {
				System.err.println(e.getLocalizedMessage());
				rarnCLI.printUsage();
				System.exit(-1);
			}
			result = rarnCLI.run();
		} catch (Throwable t) {
			System.exit(1);
		}
		if (result) {
			System.exit(0);
		}
		System.exit(2);
	}
}
