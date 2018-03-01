package org.bd.kylin.rest;

import org.apache.commons.lang3.StringUtils;
import org.bd.kylin.CubeException;
import org.bd.kylin.JobTimeFilterEnum;
import org.bd.kylin.RestRequstHandle;
import org.bd.kylin.request.JobListRequest;

/**
 * <b>版权信息:</b> 广州智数信息科技有限公司<br>
 * <b>功能描述:</b> job请求rest接口类<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年10月30日 下午2:35:40 |创建
 */
public class JobRest extends RestRequstHandle{

	/**
	 * <b>描述：</b> 获取任务列表
	 * @author wpk | 2017年10月30日 下午2:44:34 |创建
	 * @return String
	 */
	public static String getJobs(JobListRequest jlr){
		StringBuffer para = new StringBuffer();
		para.append("jobs");
		if(null == jlr){
			throw new CubeException("传入[JobListRequest]对象为空!");
		}
		if(StringUtils.isBlank(jlr.getProjectName())){
			throw new CubeException("传入[projectName]参数为空!");
		}else{
			para.append("?projectName=").append(jlr.getProjectName());
		}
		if(StringUtils.isBlank(jlr.getCubeName())){
			throw new CubeException("传入[cubeName]参数为空!");
		}else{
			para.append("&cubeName=").append(jlr.getCubeName());
		}
		if(null == jlr.getTimeFilter()){
			para.append("&timeFilter=").append(JobTimeFilterEnum.LAST_ONE_MONTH.getCode());//默认获取最近一个月的
		}else{
			para.append("&timeFilter=").append(jlr.getTimeFilter());
		}
		if(null != jlr.getStatus() && !jlr.getStatus().isEmpty()){
			para.append("&status=").append(jlr.getStatus());
		}
		if(null != jlr.getOffset()){
			para.append("&offset=").append(jlr.getOffset());
		}
		if(null != jlr.getLimit()){
			para.append("&limit=").append(jlr.getLimit());
		}
		
		String result = request(para.toString(), RequestMethod.GET);
		return result;
	}
	
	/**
	 * <b>描述：</b> 获取指定任务
	 * @author wpk | 2017年10月30日 下午2:59:58 |创建
	 * @param jobId
	 * @return String
	 */
	public static String getJob(String jobId){
		String para = "jobs/"+jobId;
		String result = request(para, RequestMethod.GET);
		return result;
	}
	
	/**
	 * <b>描述：</b> 获取作业执行每一步的日志
	 * @author wpk | 2017年10月30日 下午3:09:38 |创建
	 * @param jobId
	 * @param stepId
	 * @return String
	 */
	public static String getJobStepLog(String jobId, String stepId){
		String para = "jobs/"+jobId+"/steps/"+stepId+"/output";
		String result = request(para, RequestMethod.GET);
		return result;
	}
	
	/**
	 * <b>描述：</b> 重新执行任务（注意：如果该任务被取消获取放弃后不能再重新执行）
	 * @author wpk | 2017年10月30日 下午3:14:19 |创建
	 * @param jobId
	 * @return String
	 */
	public static String resume(String jobId){
		String para = "jobs/"+jobId+"/resume";
		String result = request(para, RequestMethod.PUT);
		return result;
	}
	
	/**
	 * <b>描述：</b> 取消/放弃一个任务
	 * @author wpk | 2017年10月30日 下午3:16:13 |创建
	 * @param jobId
	 * @return String
	 */
	public static String cancel(String jobId){
		String para = "jobs/"+jobId+"/cancel";
		String result = request(para, RequestMethod.PUT);
		return result;
	}
	
	/**
	 * <b>描述：</b> 删除任务
	 * @author wpk | 2017年10月30日 下午3:24:26 |创建
	 * @param jobId
	 * @return String
	 */
	public static String drop(String jobId){
		String para = "jobs/"+jobId+"/drop";
		String result = request(para, RequestMethod.DELETE);
		return result;
	}
	
	/**
	 * <b>描述：</b> 暂停任务
	 * @author wpk | 2017年10月30日 下午3:23:14 |创建
	 * @param jobId
	 * @return String
	 */
	public static String pause(String jobId){
		String para = "jobs/"+jobId+"/pause";
		String result = request(para, RequestMethod.PUT);
		return result;
	}
	
}
