package org.bd.kylin.response;

import java.io.Serializable;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> 查询sql返回的列元素<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年11月21日 下午5:04:02 |创建
 */
@SuppressWarnings("serial")
public class SelectedColumnMeta implements Serializable {
	
    private boolean isAutoIncrement;
    private boolean isCaseSensitive;
    private boolean isSearchable;
    private boolean isCurrency;
    private int isNullable;// 0:nonull, 1:nullable, 2: nullableunknown
    private boolean isSigned;
    private int displaySize;
    private String label;// AS keyword
    private String name;
    private String schemaName;
    private String catelogName;
    private String tableName;
    private int precision;
    private int scale;
    private int columnType;// as defined in java.sql.Types
    private String columnTypeName;
    private boolean isReadOnly;
    private boolean isWritable;
    private boolean isDefinitelyWritable;
    
	public boolean isAutoIncrement() {
		return isAutoIncrement;
	}
	public void setAutoIncrement(boolean isAutoIncrement) {
		this.isAutoIncrement = isAutoIncrement;
	}
	public boolean isCaseSensitive() {
		return isCaseSensitive;
	}
	public void setCaseSensitive(boolean isCaseSensitive) {
		this.isCaseSensitive = isCaseSensitive;
	}
	public boolean isSearchable() {
		return isSearchable;
	}
	public void setSearchable(boolean isSearchable) {
		this.isSearchable = isSearchable;
	}
	public boolean isCurrency() {
		return isCurrency;
	}
	public void setCurrency(boolean isCurrency) {
		this.isCurrency = isCurrency;
	}
	public int getIsNullable() {
		return isNullable;
	}
	public void setIsNullable(int isNullable) {
		this.isNullable = isNullable;
	}
	public boolean isSigned() {
		return isSigned;
	}
	public void setSigned(boolean isSigned) {
		this.isSigned = isSigned;
	}
	public int getDisplaySize() {
		return displaySize;
	}
	public void setDisplaySize(int displaySize) {
		this.displaySize = displaySize;
	}
	public String getLabel() {
		return label;
	}
	public void setLabel(String label) {
		this.label = label;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getSchemaName() {
		return schemaName;
	}
	public void setSchemaName(String schemaName) {
		this.schemaName = schemaName;
	}
	public String getCatelogName() {
		return catelogName;
	}
	public void setCatelogName(String catelogName) {
		this.catelogName = catelogName;
	}
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public int getPrecision() {
		return precision;
	}
	public void setPrecision(int precision) {
		this.precision = precision;
	}
	public int getScale() {
		return scale;
	}
	public void setScale(int scale) {
		this.scale = scale;
	}
	public int getColumnType() {
		return columnType;
	}
	public void setColumnType(int columnType) {
		this.columnType = columnType;
	}
	public String getColumnTypeName() {
		return columnTypeName;
	}
	public void setColumnTypeName(String columnTypeName) {
		this.columnTypeName = columnTypeName;
	}
	public boolean isReadOnly() {
		return isReadOnly;
	}
	public void setReadOnly(boolean isReadOnly) {
		this.isReadOnly = isReadOnly;
	}
	public boolean isWritable() {
		return isWritable;
	}
	public void setWritable(boolean isWritable) {
		this.isWritable = isWritable;
	}
	public boolean isDefinitelyWritable() {
		return isDefinitelyWritable;
	}
	public void setDefinitelyWritable(boolean isDefinitelyWritable) {
		this.isDefinitelyWritable = isDefinitelyWritable;
	}
	
}
