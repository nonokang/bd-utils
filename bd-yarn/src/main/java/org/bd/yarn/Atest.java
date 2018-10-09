package org.bd.yarn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.applications.distributedshell.DSConstants;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.bd.hdfs.HdfsClient;

public class Atest {
	
	public static Configuration conf = null;
	public static FileStatus destStatus = null;

	public static void main(String[] args) throws YarnException, IOException {
		conf = new Configuration();
		YarnClient client = YarnClient.createYarnClient();
		client.init(conf);
		client.start();
		
		//创建一个Application
		YarnClientApplication app = client.createApplication();
		
		//构造ApplicationSubmissionContext， 用以提交作业
		ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
		ApplicationId appId = appContext.getApplicationId();
		System.out.println("yarn id====="+appId);

		appContext.setApplicationName("wpktest-01");//设置名称
		appContext.setResource(Resource.newInstance(100, 1));//设置内存和CPU需求
		appContext.setPriority(Priority.newInstance(0));//设置优先级
		appContext.setQueue("default");//设置queue信息
		appContext.setAMContainerSpec(getContext());
		
		appContext.setKeepContainersAcrossApplicationAttempts(true);
		appContext.setMaxAppAttempts(5);
		//设置应用类型
		if (appContext.getApplicationType() == null) {
			appContext.setApplicationType(YarnConfiguration.DEFAULT_APPLICATION_TYPE);
		} else {
		    if (appContext.getApplicationType().length() > YarnConfiguration.APPLICATION_TYPE_LENGTH) {
		    	appContext.setApplicationType(appContext.getApplicationType().substring(0,
		          YarnConfiguration.APPLICATION_TYPE_LENGTH));
		    }
		}

		
		client.submitApplication(appContext);//将应用程序提交到ResouceManager上

	}

	//构造ApplicationSubmissionContext， 用以提交作业
	public static ContainerLaunchContext getContext(){
		//构造AM的container，加载上下文，包含本地资源，环境变量，实际命令
	    ContainerLaunchContext container = Records.newRecord(ContainerLaunchContext.class);
        //添加localResource到amContainer
		container.setLocalResources(getLocalResources());
		//添加环境变量到amContainer
	    container.setEnvironment(getEnvironment());
	    //添加执行命令到amContainer
        container.setCommands(getCommands());
        /*ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance(
        		getLocalResources(), getEnvironment(), getCommands(), null, null, null);*/
		return container;
	}
	
	public static Map<String,LocalResource> getLocalResources(){
	    //AM需要的本地资源，如jar包、log文件
		Map<String,LocalResource> localResources = new HashMap<String,LocalResource>();

		HdfsClient item = new HdfsClient();
        Path dst = new Path("/backup/bd-mapreduce-0.0.1-SNAPSHOT.jar");
        /*item.uploadFile("C:\\Users\\ennwpae\\Desktop\\bd-mapreduce-0.0.1-SNAPSHOT.jar", "/backup");*/
		destStatus = item.getFileStatus("/backup/bd-mapreduce-0.0.1-SNAPSHOT.jar");

        LocalResource scRsrc = LocalResource.newInstance(ConverterUtils.getYarnUrlFromURI(dst.toUri()),
                LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
                destStatus.getLen(), destStatus.getModificationTime());

        localResources.put("bd-mapreduce-0.0.1-SNAPSHOT.jar", scRsrc);
        return localResources;
	}
	
	public static Map<String, String> getEnvironment(){
		//设置环境变量
		Map<String, String> env = new HashMap<String, String>();
	    env.put(DSConstants.DISTRIBUTEDSHELLSCRIPTLOCATION, "/backup/bd-mapreduce-0.0.1-SNAPSHOT.jar");
	    env.put(DSConstants.DISTRIBUTEDSHELLSCRIPTTIMESTAMP, Long.toString(destStatus.getModificationTime()));
	    env.put(DSConstants.DISTRIBUTEDSHELLSCRIPTLEN, Long.toString(destStatus.getLen()));

	    StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$$())
	        .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
	    for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
	    		YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
	      classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
	      classPathEnv.append(c.trim());
	    }
	    classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./log4j.properties");
	    env.put("CLASSPATH", classPathEnv.toString());
		return env;
	}
	
	public static List<String> getCommands(){
	    //设置命令
		// 设置必要的命令来执行master应用程序
	    Vector<CharSequence> vargs = new Vector<CharSequence>(30);

	    // 设置java可执行的命令
	    vargs.add(Environment.JAVA_HOME.$$() + "/bin/java");
	    // 根据AM内存大小设置Xmx
	    vargs.add("-Xmx" + 10 + "m");
	    // 设置 class name
	    vargs.add("org.bd.mapreduce.WpkCount");
	    // 设置 Master应用参数
	    vargs.add("--container_memory " + String.valueOf(1));
	    vargs.add("--container_vcores " + String.valueOf(1));
	    vargs.add("--num_containers " + String.valueOf(1));
	    vargs.add("--priority " + String.valueOf(0));
	    vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
	    vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");

	    // Get final command
	    StringBuilder command = new StringBuilder();
	    for (CharSequence str : vargs) {
	      command.append(str).append(" ");
	    }
	    List<String> commands = new ArrayList<String>();
	    commands.add(command.toString());
        //添加命令行到amContainer
		return commands;
	}

}
