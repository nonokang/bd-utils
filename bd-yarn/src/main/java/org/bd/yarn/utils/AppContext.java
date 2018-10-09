package org.bd.yarn.utils;

public class AppContext {

	// 设置应用名称
	private String appName = "Hello_WPK";

	// 设置应用类型(默认为YARN)
	private String appType = "YARN";

	// 设置应用优先级(数值越小，优先级越高)
	private int amPriority = 0;

	// 设置应用队列(默认为“default”，不同的应用可以任务在不同的队列中运行，目的是为了合理的利用资源，专业名词叫资源隔离)
	private String amQueue = "default";

	// 设置应用内存（单位为M）
	private int amMemory = 10;

	// 设置虚拟CPU
	private int amVCores = 1;

	// 设置应用的jar文件的路径，此jar文件需要在hdfs上（注意是hdfs的绝对路径）
	private String appJarPath = "";

	// 设置应用执行时的类名（包名+类名。如org.bd.yarn.utils.AppContext）
	private String appClassName = "";

	// 设置容器的优先级
	private int containerPriority = 0;

	// 设置容器内存（单位为M）
	private int containerMemory = 10;

	// 设置容器虚拟CPU
	private int containerVCores = 1;

	// 设置容器数量
	private int numContainers = 1;

	// 设置失败尝试次数（AM启动失败后，最大的尝试重启次数）
	private int maxAppAttempts = 0;

	public String getAppName() {
		return appName;
	}

	public void setAppName(String appName) {
		this.appName = appName;
	}

	public String getAppType() {
		return appType;
	}

	public void setAppType(String appType) {
		this.appType = appType;
	}

	public int getAmPriority() {
		return amPriority;
	}

	public void setAmPriority(int amPriority) {
		this.amPriority = amPriority;
	}

	public String getAmQueue() {
		return amQueue;
	}

	public void setAmQueue(String amQueue) {
		this.amQueue = amQueue;
	}

	public int getAmMemory() {
		return amMemory;
	}

	public void setAmMemory(int amMemory) {
		this.amMemory = amMemory;
	}

	public int getAmVCores() {
		return amVCores;
	}

	public void setAmVCores(int amVCores) {
		this.amVCores = amVCores;
	}

	public String getAppJarPath() {
		return appJarPath;
	}

	public void setAppJarPath(String appJarPath) {
		this.appJarPath = appJarPath;
	}

	public String getAppClassName() {
		return appClassName;
	}

	public void setAppClassName(String appClassName) {
		this.appClassName = appClassName;
	}

	public int getContainerPriority() {
		return containerPriority;
	}

	public void setContainerPriority(int containerPriority) {
		this.containerPriority = containerPriority;
	}

	public int getContainerMemory() {
		return containerMemory;
	}

	public void setContainerMemory(int containerMemory) {
		this.containerMemory = containerMemory;
	}

	public int getContainerVCores() {
		return containerVCores;
	}

	public void setContainerVCores(int containerVCores) {
		this.containerVCores = containerVCores;
	}

	public int getNumContainers() {
		return numContainers;
	}

	public void setNumContainers(int numContainers) {
		this.numContainers = numContainers;
	}

	public int getMaxAppAttempts() {
		return maxAppAttempts;
	}

	public void setMaxAppAttempts(int maxAppAttempts) {
		this.maxAppAttempts = maxAppAttempts;
	}
	  
}
