package org.bd.kylin;

/**
 * <b>版权信息:</b> big data module<br>
 * <b>功能描述:</b> <br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年11月21日 上午11:43:42 |创建
 */
public enum CubeBuildTypeEnum {
    /**
     * 重建节段或者增量节段
     */
    BUILD,
    /**
     * 合并节段
     */
    MERGE,

    /**
     * 刷新节段
     */
    REFRESH
}
