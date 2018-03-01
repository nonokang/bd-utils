package org.bd.impala;

import java.util.List;

/**
 * <b>版权信息:</b> 广州智数信息科技有限公司<br>
 * <b>功能描述:</b> impala表信息对象类<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年12月12日 下午9:59:54 |创建
 */
public class ImpalaTableMeta {

	/** 表名*/
    private String tableName;//表名
    /** 存储位置*/
    private String location;
    /** 输入格式*/
    private String inputFormat;
    /** 输出格式*/
    private String outputFormat;
    /** 所有者*/
    private String owner;
    /** 表类型*/
    private String tableType;
    /** 跳过标题数*/
    private int skipHeaderLineCount;
    /** 上一次使用时间*/
    private int lastAccessTime;
    /** 文件大小*/
    private long fileSize;
    /** 文件数*/
    private long fileNum;
    /** 是否本地*/
    private boolean isNative;
    /** 所有字段（包括分区字段）*/
    private List<ImpalaTableColumnMeta> fieldColumns;
    /** 分区字段*/
    private List<ImpalaTableColumnMeta> partitionColumns;
    
    public ImpalaTableMeta(){}

    public ImpalaTableMeta(String tableName, String location, String inputFormat, 
    		String outputFormat, String owner, String tableType, int lastAccessTime, 
    		long fileSize, long fileNum, int skipHeaderLineCount, boolean isNative, 
    		List<ImpalaTableColumnMeta> fieldColumns, List<ImpalaTableColumnMeta> partitionColumns) {
        this.tableName = tableName;
        this.location = location;
        this.inputFormat = inputFormat;
        this.outputFormat = outputFormat;
        this.owner = owner;
        this.tableType = tableType;
        this.lastAccessTime = lastAccessTime;
        this.fileSize = fileSize;
        this.fileNum = fileNum;
        this.isNative = isNative;
        this.fieldColumns = fieldColumns;
        this.partitionColumns = partitionColumns;
        this.skipHeaderLineCount = skipHeaderLineCount;
    }

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public void setInputFormat(String inputFormat) {
		this.inputFormat = inputFormat;
	}

	public void setOutputFormat(String outputFormat) {
		this.outputFormat = outputFormat;
	}

	public void setOwner(String owner) {
		this.owner = owner;
	}

	public void setTableType(String tableType) {
		this.tableType = tableType;
	}

	public void setSkipHeaderLineCount(int skipHeaderLineCount) {
		this.skipHeaderLineCount = skipHeaderLineCount;
	}

	public void setLastAccessTime(int lastAccessTime) {
		this.lastAccessTime = lastAccessTime;
	}

	public void setFileSize(long fileSize) {
		this.fileSize = fileSize;
	}

	public void setFileNum(long fileNum) {
		this.fileNum = fileNum;
	}

	public void setNative(boolean isNative) {
		this.isNative = isNative;
	}

	public void setFieldColumns(List<ImpalaTableColumnMeta> fieldColumns) {
		this.fieldColumns = fieldColumns;
	}

	public void setPartitionColumns(List<ImpalaTableColumnMeta> partitionColumns) {
		this.partitionColumns = partitionColumns;
	}
    
	/**
	 * <b>描述：</b> 获取impala表信息
	 * @author wpk | 2017年12月12日 下午10:02:30 |创建
	 * @return ImpalaTableMeta
	 */
    public ImpalaTableMeta buildImpalaTableMeta(){
    	return new ImpalaTableMeta(tableName, location, inputFormat, outputFormat, owner, tableType, 
    			lastAccessTime, fileSize, fileNum, skipHeaderLineCount, isNative, fieldColumns, partitionColumns);
    }

	/**
	 * <b>版权信息:</b> 广州智数信息科技有限公司<br>
	 * <b>功能描述:</b> impala表的列信息对象类<br>
	 * <b>版本历史:</b>
	 * @author  wpk | 2017年12月12日 下午10:02:52 |创建
	 */
    public static class ImpalaTableColumnMeta {
        String name;
        String dataType;
        String comment;

        public ImpalaTableColumnMeta(String name, String dataType, String comment) {
            this.name = name;
            this.dataType = dataType;
            this.comment = comment;
        }

        @Override
        public String toString() {
            return "HiveTableColumnMeta{name='" + name + "', dataType='" + dataType + "', comment='" + comment + "'}";
        }
    }

    @Override
    public String toString() {
        return "HiveTableMeta{tableName='" + tableName + "', location='" + location +  
        		"', inputFormat='" + inputFormat + "', outputFormat='" + outputFormat + 
        		"', owner='" + owner + "', tableType='" + tableType + "', skipHeaderLineCount=" + skipHeaderLineCount +  
        		", lastAccessTime=" + lastAccessTime + ", fileSize=" + fileSize + ", fileNum=" + fileNum + 
        		", isNative=" + isNative + ", fieldColumns=" + fieldColumns + ", partitionColumns=" + partitionColumns + "}";
    }
}
