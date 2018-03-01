package org.bd.datax.bean;

/**
 * <b>版权信息:</b> 广州智数信息科技有限公司<br>
 * <b>功能描述:</b> data全局参数设置类<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年9月20日 下午9:08:06 |创建
 */
public class Setting {

	private Speed speed;//设置传输速度 byte/s 尽量逼近这个速度但是不高于它
	private ErrorLimit errorLimit;//出错限制
	
	public class Speed{
		private Integer channel = 1;//表示通道数量，byte表示通道速度，如果单通道速度1MB，配置byte为1048576表示一个channel
//		private Integer _byte = 1048576;
		
		public Integer getChannel() {
			return channel;
		}
		public void setChannel(Integer channel) {
			this.channel = channel;
		}
		/*public Integer get_byte() {
			return _byte;
		}
		public void set_byte(Integer _byte) {
			this._byte = _byte;
		}*/
	}
	
	public class ErrorLimit{
		private Integer record = 0;//先选择record
		private Double percentage = 0.02;//百分比  1表示100%
		
		public Integer getRecord() {
			return record;
		}
		public void setRecord(Integer record) {
			this.record = record;
		}
		public Double getPercentage() {
			return percentage;
		}
		public void setPercentage(Double percentage) {
			this.percentage = percentage;
		}
	}

	public Speed getSpeed() {
		return speed;
	}

	public void setSpeed(Speed speed) {
		this.speed = speed;
	}

	public ErrorLimit getErrorLimit() {
		return errorLimit;
	}

	public void setErrorLimit(ErrorLimit errorLimit) {
		this.errorLimit = errorLimit;
	}
	
}
