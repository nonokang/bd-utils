package org.bd.kylin;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> 作业的时间过滤对象<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年11月21日 上午10:25:39 |创建
 */
public enum JobTimeFilterEnum {
	
    LAST_ONE_DAY(0), LAST_ONE_WEEK(1), LAST_ONE_MONTH(2), LAST_ONE_YEAR(3), ALL(4);

    private final int code;

    private JobTimeFilterEnum(int code) {
        this.code = code;
    }

    public static JobTimeFilterEnum getByCode(int code) {
        for (JobTimeFilterEnum timeFilter : values()) {
            if (timeFilter.getCode() == code) {
                return timeFilter;
            }
        }

        return null;
    }

    public int getCode() {
        return code;
    }
}
