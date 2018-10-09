package org.bd.yarn.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> 获取yarn的应用信息工具类<br>
 * <b>版本历史:</b>
 * @author  wpk | 2018年9月1日 下午4:34:59 |创建
 */
public class YarnAppInfo {
	
	private YarnClient client;
	
	public YarnAppInfo(){
		YarnUtil yarn = new YarnUtil();
		yarn.getClient();
	}

	/**
	 * <b>描述：</b> 通过yarn应用ID获取信息
	 * @author wpk | 2018年9月1日 下午4:56:08 |创建
	 * @param appId
	 * @return
	 */
	public ApplicationReport getAppById(ApplicationId appId){
		try {
			return client.getApplicationReport(appId);
		} catch (YarnException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * <b>描述：</b> 通过指定yarn的状态获取应用信息
	 * @author wpk | 2018年9月1日 下午4:56:54 |创建
	 * @param status
	 * @return
	 */
	public List<ApplicationReport> getAppByStatus(YarnApplicationState status){
		EnumSet<YarnApplicationState> appStates = EnumSet.noneOf(YarnApplicationState.class);
		appStates.add(status);
		return getAppsByStatus(appStates);
	}
	
	/**
	 * <b>描述：</b> 通过指定多个yarn的状态获取应用信息
	 * @author wpk | 2018年9月1日 下午4:57:20 |创建
	 * @param status
	 * @return
	 */
	public List<ApplicationReport> getAppsByStatus(EnumSet<YarnApplicationState> status){
		try {
			return client.getApplications(status);
		} catch (YarnException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * <b>描述：</b> 通过指定尝试ID获取应用尝试信息
	 * @author wpk | 2018年9月1日 下午5:43:58 |创建
	 * @return
	 */
	public ApplicationAttemptReport getAppAttempByAttId(ApplicationAttemptId attId){
		try {
			return client.getApplicationAttemptReport(attId);
		} catch (YarnException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * <b>描述：</b> 通过指定yarn应用ID获取所有的应用尝试信息
	 * @author wpk | 2018年9月1日 下午5:47:23 |创建
	 * @param appId
	 * @return
	 */
	public List<ApplicationAttemptReport> getAppAttempByAppId(ApplicationId appId){
		try {
			return client.getApplicationAttempts(appId);
		} catch (YarnException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * <b>描述：</b> 通过容器ID获取容器信息
	 * @author wpk | 2018年9月1日 下午9:59:02 |创建
	 * @param containerId
	 * @return
	 */
	public ContainerReport getContainerById(ContainerId containerId){
		try {
			return client.getContainerReport(containerId);
			
		} catch (YarnException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * <b>描述：</b> 通过应用尝试ID获取所有的容器信息
	 * @author wpk | 2018年9月1日 下午10:02:48 |创建
	 * @param attId
	 * @return
	 */
	public List<ContainerReport> getContainerByAttId(ApplicationAttemptId attId){
		try {
			return client.getContainers(attId);
		} catch (YarnException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * <b>描述：</b> 通过应用ID获取所有的容器信息
	 * @author wpk | 2018年9月1日 下午10:08:43 |创建
	 * @param appId
	 * @return
	 */
	public List<ContainerReport> getContainerByAppId(ApplicationId appId){
		List<ContainerReport> all = new ArrayList<ContainerReport>();
		List<ApplicationAttemptReport> aaps = getAppAttempByAppId(appId);
		for(ApplicationAttemptReport aap : aaps){
			List<ContainerReport> portion = getContainerByAttId(aap.getApplicationAttemptId());
			all.addAll(portion);
		}
		return all;
	}
	
}
