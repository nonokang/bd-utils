package org.bd.yarn.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.applications.distributedshell.DSConstants;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.bd.hdfs.HdfsClient;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> yarn的应用操作<br>
 * <b>版本历史:</b>
 * @author  wpk | 2018年9月1日 下午10:23:05 |创建
 */
public class YarnAppHandle {

	private YarnUtil yarnUtil;
	private YarnAppInfo yarnAppInfo;
	private YarnClient client;
	
	public YarnAppHandle(){
		yarnUtil = new YarnUtil();
		yarnAppInfo = new YarnAppInfo();
		client = yarnUtil.getClient();
	}
	
	/**
	 * <b>描述：</b> 通过应用ID停止应用
	 * @author wpk | 2018年9月1日 下午11:00:26 |创建
	 * @param appId
	 */
	public void killAppById(String appId){
		ApplicationId applicationId = ConverterUtils.toApplicationId(appId);
	    ApplicationReport appReport = yarnAppInfo.getAppById(applicationId);

	    try {
		    if (appReport.getYarnApplicationState() == YarnApplicationState.FINISHED
			        || appReport.getYarnApplicationState() == YarnApplicationState.KILLED
			        || appReport.getYarnApplicationState() == YarnApplicationState.FAILED) {
			    System.out.println("Application " + applicationId + " has already finished ");
			} else {
				client.killApplication(applicationId);
			}
		} catch (YarnException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * <b>描述：</b> 将应用程序提交到ResouceManager上
	 * @author wpk | 2018年9月1日 下午11:48:46 |创建
	 * @param appContext
	 */
	public ApplicationId submitApp(AppContext appContext){
		try {
			ApplicationSubmissionContext asc = appContextJudge(appContext);
			return client.submitApplication(asc);
		} catch (YarnException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * <b>描述：</b> 获取应用文本对象
	 * @author wpk | 2018年9月1日 下午11:50:50 |创建
	 * @return
	 */
	public ApplicationSubmissionContext getAppContext(){
		try {
			//创建一个Application
			YarnClientApplication app = client.createApplication();
			//构造ApplicationSubmissionContext， 用以提交作业
			return app.getApplicationSubmissionContext();
		} catch (YarnException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * <b>描述：</b> 应用文本的判断
	 * @author wpk | 2018年9月2日 下午4:22:19 |创建
	 * @param appContext
	 * @return
	 * @throws YarnException 
	 */
	private ApplicationSubmissionContext appContextJudge(AppContext appContext) throws YarnException{
		ApplicationSubmissionContext asc = getAppContext();
		
		asc.setApplicationName(appContext.getAppName());//设置名称
		asc.setResource(Resource.newInstance(appContext.getAmMemory(), appContext.getAmVCores()));//设置内存和CPU需求
		asc.setPriority(Priority.newInstance(appContext.getAmPriority()));//设置优先级
		asc.setMaxAppAttempts(appContext.getMaxAppAttempts());//设置失败尝试数
		asc.setQueue(appContext.getAmQueue());//设置queue信息
		
		//设置应用类型
		if (appContext.getAppType() == null || "".equals(appContext.getAppType())) {
			asc.setApplicationType(YarnConfiguration.DEFAULT_APPLICATION_TYPE);
		} else {
		    if (appContext.getAppType().length() > YarnConfiguration.APPLICATION_TYPE_LENGTH) {
		    	asc.setApplicationType(appContext.getAppType().substring(0,
		          YarnConfiguration.APPLICATION_TYPE_LENGTH));
		    }
		}
		asc.setAMContainerSpec(getContainerContext(appContext));//设置容器信息
		return asc;
	}
	
	/**
	 * <b>描述：</b> 获取容器文本
	 * @author wpk | 2018年9月2日 下午4:25:55 |创建
	 * @param appContext
	 * @return
	 * @throws YarnException 
	 */
	private ContainerLaunchContext getContainerContext(AppContext appContext) throws YarnException{
		//构造AM的container，加载上下文，包含本地资源，环境变量，实际命令
	    ContainerLaunchContext container = Records.newRecord(ContainerLaunchContext.class);
        //添加localResource到amContainer
		container.setLocalResources(localResourcesJudge(appContext));
		//添加环境变量到amContainer
	    container.setEnvironment(environmentJudge(appContext));
	    //添加执行命令到amContainer
        container.setCommands(commandsJudge(appContext));
        /*ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance(
        		getLocalResources(), getEnvironment(), getCommands(), null, null, null);*/
		return container;
	}
	
	private FileStatus getFileStatus(AppContext appContext) throws YarnException{
		HdfsClient item = new HdfsClient();
        boolean flag = item.isDirExists(appContext.getAppJarPath());
        FileStatus destStatus = null;
        if(flag){
    		destStatus = item.getFileStatus(appContext.getAppJarPath());
        }else{
			throw new YarnException("在hdfs上不存在'"+appContext.getAppJarPath()+"'文件");
        }
        return destStatus;
	}
	
	/**
	 * <b>描述：</b> 判断容器所需的本地资源
	 * @author wpk | 2018年9月2日 下午4:52:10 |创建
	 * @param appContext
	 * @return
	 * @throws YarnException 
	 */
	private Map<String,LocalResource> localResourcesJudge(AppContext appContext) throws YarnException{
	    //AM需要的本地资源，如jar包、log文件
		Map<String,LocalResource> localResources = new HashMap<String,LocalResource>();
		FileStatus destStatus = getFileStatus(appContext);
        Path dst = new Path(appContext.getAppJarPath());
        LocalResource scRsrc = LocalResource.newInstance(ConverterUtils.getYarnUrlFromURI(dst.toUri()),
                LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
                destStatus.getLen(), destStatus.getModificationTime());

        localResources.put("bd-mapreduce-0.0.1-SNAPSHOT.jar", scRsrc);
        return localResources;
	}
	
	/**
	 * <b>描述：</b> 判断容器所需的环境变量
	 * @author wpk | 2018年9月2日 下午4:54:30 |创建
	 * @return
	 * @throws YarnException 
	 */
	private Map<String, String> environmentJudge(AppContext appContext) throws YarnException{
		//设置环境变量
		Map<String, String> env = new HashMap<String, String>();
		FileStatus destStatus = getFileStatus(appContext);
	    env.put(DSConstants.DISTRIBUTEDSHELLSCRIPTLOCATION, appContext.getAppJarPath());
	    env.put(DSConstants.DISTRIBUTEDSHELLSCRIPTTIMESTAMP, Long.toString(destStatus.getModificationTime()));
	    env.put(DSConstants.DISTRIBUTEDSHELLSCRIPTLEN, Long.toString(destStatus.getLen()));

	    StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$$())
	        .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
	    for (String c : yarnUtil.getConfig().getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
	    		YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
	      classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
	      classPathEnv.append(c.trim());
	    }
	    classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./log4j.properties");
	    env.put("CLASSPATH", classPathEnv.toString());
		return env;
	}

	/**
	 * <b>描述：</b> 判断容器所需的命令
	 * @author wpk | 2018年9月2日 下午5:08:20 |创建
	 * @return
	 */
	private List<String> commandsJudge(AppContext appContext){
		// 设置必要的命令来执行master应用程序
	    Vector<CharSequence> vargs = new Vector<CharSequence>(30);

	    // 设置java可执行的命令
	    vargs.add(Environment.JAVA_HOME.$$() + "/bin/java");
	    // 根据AM内存大小设置Xmx
	    vargs.add("-Xmx" + appContext.getAmMemory() + "m");
	    // 设置 class name
	    vargs.add(appContext.getAppClassName());
	    // 设置 Master应用参数
	    vargs.add("--container_memory " + String.valueOf(appContext.getContainerMemory()));
	    vargs.add("--container_vcores " + String.valueOf(appContext.getContainerVCores()));
	    vargs.add("--num_containers " + String.valueOf(appContext.getNumContainers()));
	    vargs.add("--priority " + String.valueOf(appContext.getContainerPriority()));
	    vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/WpkAppMaster.stdout");
	    vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/WpkAppMaster.stderr");

	    // 拼接命令
	    StringBuilder command = new StringBuilder();
	    for (CharSequence str : vargs) {
	      command.append(str).append(" ");
	    }
	    List<String> commands = new ArrayList<String>();
	    commands.add(command.toString());
		return commands;
	}
}
