package org.bd.kylin.response;

import java.io.Serializable;
import java.util.List;

/**
 * <b>版权信息:</b> 广州智数信息科技有限公司<br>
 * <b>功能描述:</b> 查询sql返回的信息<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年11月21日 下午5:03:27 |创建
 */
public class SQLResponse implements Serializable {
	
    private static final long serialVersionUID = 1L;

    private List<SelectedColumnMeta> columnMetas;
    private List<List<String>> results;
    private String cube;
    private int affectedRowCount;
    private boolean isException;
    private String exceptionMessage;
    private long duration;
    private boolean isPartial = false;
    private long totalScanCount;
    private long totalScanBytes;
    private boolean hitExceptionCache = false;
    private boolean storageCacheUsed = false;
    private boolean queryPushDown = false;
    
    public SQLResponse() {
    }

    public SQLResponse(List<SelectedColumnMeta> columnMetas, List<List<String>> results, int affectedRowCount,
            boolean isException, String exceptionMessage) {
        this.columnMetas = columnMetas;
        this.results = results;
        this.affectedRowCount = affectedRowCount;
        this.isException = isException;
        this.exceptionMessage = exceptionMessage;
    }

    public SQLResponse(List<SelectedColumnMeta> columnMetas, List<List<String>> results, String cube,
            int affectedRowCount, boolean isException, String exceptionMessage) {
        this.columnMetas = columnMetas;
        this.results = results;
        this.cube = cube;
        this.affectedRowCount = affectedRowCount;
        this.isException = isException;
        this.exceptionMessage = exceptionMessage;
    }

    public SQLResponse(List<SelectedColumnMeta> columnMetas, List<List<String>> results, String cube,
            int affectedRowCount, boolean isException, String exceptionMessage, boolean isPartial, boolean isPushDown) {
        this.columnMetas = columnMetas;
        this.results = results;
        this.cube = cube;
        this.affectedRowCount = affectedRowCount;
        this.isException = isException;
        this.exceptionMessage = exceptionMessage;
        this.isPartial = isPartial;
        this.queryPushDown = isPushDown;
    }

    public List<SelectedColumnMeta> getColumnMetas() {
        return columnMetas;
    }

    public List<List<String>> getResults() {
        return results;
    }

    public void setResults(List<List<String>> results) {
        this.results = results;
    }

    public String getCube() {
        return cube;
    }

    public void setCube(String cube) {
        this.cube = cube;
    }

    public int getAffectedRowCount() {
        return affectedRowCount;
    }

    public boolean getIsException() {
        return isException;
    }

    public void setIsException(boolean v) {
        isException = v;
    }

    public String getExceptionMessage() {
        return exceptionMessage;
    }

    public void setExceptionMessage(String msg) {
        exceptionMessage = msg;
    }

    public long getDuration() {
        return duration;
    }

    public void setDuration(long duration) {
        this.duration = duration;
    }

    public boolean isPartial() {

        return isPartial;
    }

    public boolean isPushDown() {
        return queryPushDown;
    }

    public long getTotalScanCount() {
        return totalScanCount;
    }

    public void setTotalScanCount(long totalScanCount) {
        this.totalScanCount = totalScanCount;
    }

    public long getTotalScanBytes() {
        return totalScanBytes;
    }

    public void setTotalScanBytes(long totalScanBytes) {
        this.totalScanBytes = totalScanBytes;
    }

    public boolean isHitExceptionCache() {
        return hitExceptionCache;
    }

    public void setHitExceptionCache(boolean hitExceptionCache) {
        this.hitExceptionCache = hitExceptionCache;
    }

    public boolean isStorageCacheUsed() {
        return storageCacheUsed;
    }

    public void setStorageCacheUsed(boolean storageCacheUsed) {
        this.storageCacheUsed = storageCacheUsed;
    }
}
