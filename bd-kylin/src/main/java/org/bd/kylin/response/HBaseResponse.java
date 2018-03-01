package org.bd.kylin.response;

/**
 * <b>版权信息:</b> 广州智数信息科技有限公司<br>
 * <b>功能描述:</b> <br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年11月21日 上午9:53:30 |创建
 */
public class HBaseResponse {
    
    private String segmentName;
    private String segmentStatus;
    private String tableName;
    private long tableSize;
    private int regionCount;
    private long dateRangeStart;
    private long dateRangeEnd;
    private long sourceOffsetStart;
    private long sourceOffsetEnd;
    private long sourceCount;

    public HBaseResponse() {
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public long getTableSize() {
        return tableSize;
    }

    public void setTableSize(long tableSize) {
        this.tableSize = tableSize;
    }

    public int getRegionCount() {
        return regionCount;
    }

    public void setRegionCount(int regionCount) {
        this.regionCount = regionCount;
    }

    public long getDateRangeStart() {
        return dateRangeStart;
    }

    public void setDateRangeStart(long dateRangeStart) {
        this.dateRangeStart = dateRangeStart;
    }

    public long getDateRangeEnd() {
        return dateRangeEnd;
    }

    public void setDateRangeEnd(long dateRangeEnd) {
        this.dateRangeEnd = dateRangeEnd;
    }

    public String getSegmentName() {
        return segmentName;
    }

    public void setSegmentName(String segmentName) {
        this.segmentName = segmentName;
    }

    public String getSegmentStatus() {
        return segmentStatus;
    }

    public void setSegmentStatus(String segmentStatus) {
        this.segmentStatus = segmentStatus;
    }

    public long getSourceOffsetStart() {
        return sourceOffsetStart;
    }

    public void setSourceOffsetStart(long sourceOffsetStart) {
        this.sourceOffsetStart = sourceOffsetStart;
    }

    public long getSourceOffsetEnd() {
        return sourceOffsetEnd;
    }

    public void setSourceOffsetEnd(long sourceOffsetEnd) {
        this.sourceOffsetEnd = sourceOffsetEnd;
    }

    public long getSourceCount() {
        return sourceCount;
    }

    public void setSourceCount(long sourceCount) {
        this.sourceCount = sourceCount;
    }
}
