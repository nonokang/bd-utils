package org.bd.hive;

import java.util.List;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> hive表信息对象类<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年10月31日 上午10:12:37 |创建
 */
public class HiveTableMeta {

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
    private List<HiveTableColumnMeta> fieldColumns;
    /** 分区字段*/
    private List<HiveTableColumnMeta> partitionColumns;
    
    public HiveTableMeta(){}

    public HiveTableMeta(String tableName, String location, String inputFormat, 
    		String outputFormat, String owner, String tableType, int lastAccessTime, 
    		long fileSize, long fileNum, int skipHeaderLineCount, boolean isNative, 
    		List<HiveTableColumnMeta> fieldColumns, List<HiveTableColumnMeta> partitionColumns) {
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

	public void setFieldColumns(List<HiveTableColumnMeta> fieldColumns) {
		this.fieldColumns = fieldColumns;
	}

	public void setPartitionColumns(List<HiveTableColumnMeta> partitionColumns) {
		this.partitionColumns = partitionColumns;
	}
    
	/**
	 * <b>描述：</b> 获取hive表信息
	 * @author wpk | 2017年10月31日 上午11:50:39 |创建
	 * @return HiveTableMeta
	 */
    public HiveTableMeta buildHiveTableMeta(){
    	return new HiveTableMeta(tableName, location, inputFormat, outputFormat, owner, tableType, 
    			lastAccessTime, fileSize, fileNum, skipHeaderLineCount, isNative, fieldColumns, partitionColumns);
    }

	/**
     * <b>版权信息:</b> big data module<br>
     * <b>功能描述:</b> hive表的列信息对象类<br>
     * <b>版本历史:</b>
     * @author  wpk | 2017年10月31日 上午10:13:44 |创建
     */
    public static class HiveTableColumnMeta {
        String name;
        String dataType;
        String comment;

        public HiveTableColumnMeta(String name, String dataType, String comment) {
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
