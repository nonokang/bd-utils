package org.bd.impala.utils;

import org.apache.commons.lang3.StringUtils;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> impala表的结构对象<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年8月23日 上午11:21:36 |创建
 */
@Deprecated
public class ImpaIaTableColumn {
	
	public final static String COLUMN_NAME = "name";
	public final static String COLUMN_TYPE = "type";
	public final static String COLUMN_COMMENT = "comment";
	
	private String name = "";
	private String type = "string";
	private String comment = "";
	
	public ImpaIaTableColumn(){
	}
	public ImpaIaTableColumn(String name){
		this.name = name.trim();
	}
	public ImpaIaTableColumn(String name, String type){
		this.name = name.trim();
		this.type = type.trim();
	}
	public ImpaIaTableColumn(String name, String type, String comment){
		this.name = name.trim();
		this.type = type.trim();
		this.comment = comment.trim();
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name.trim();
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		if(StringUtils.isNotEmpty(type.trim())){
			this.type = type.trim();
		}
	}
	public String getComment() {
		return comment;
	}
	public void setComment(String comment) {
		this.comment = comment.trim();
	}

}
