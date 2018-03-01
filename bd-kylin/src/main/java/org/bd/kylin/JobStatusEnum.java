package org.bd.kylin;

/**
 * <b>版权信息:</b> 广州智数信息科技有限公司<br>
 * <b>功能描述:</b> 作业状态<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年11月21日 上午10:21:31 |创建
 */
public enum JobStatusEnum {

    NEW(0), PENDING(1), RUNNING(2), FINISHED(4), ERROR(8), DISCARDED(16), STOPPED(32);

    private final int code;

    private JobStatusEnum(int statusCode) {
        this.code = statusCode;
    }

    public static JobStatusEnum getByCode(int statusCode) {
        for (JobStatusEnum status : values()) {
            if (status.getCode() == statusCode) {
                return status;
            }
        }

        return null;
    }

    public int getCode() {
        return this.code;
    }

    public boolean isComplete() {
        return code == JobStatusEnum.FINISHED.getCode() || code == JobStatusEnum.ERROR.getCode() || code == JobStatusEnum.DISCARDED.getCode();
    }

}
