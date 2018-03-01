package org.bd.datax.bean;

import java.util.List;

/**
 * <b>版权信息:</b> 广州智数信息科技有限公司<br>
 * <b>功能描述:</b> datax内容设置类<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年9月20日 下午9:18:32 |创建
 */
public class Content {

	private Read reader;
	private List<Transformer> transformer;
	private Write writer;
	
	public Read getReader() {
		return reader;
	}
	public void setReader(Read reader) {
		this.reader = reader;
	}
	public List<Transformer> getTransformer() {
		return transformer;
	}
	public void setTransformer(List<Transformer> transformer) {
		this.transformer = transformer;
	}
	public Write getWriter() {
		return writer;
	}
	public void setWriter(Write writer) {
		this.writer = writer;
	}
	
}
