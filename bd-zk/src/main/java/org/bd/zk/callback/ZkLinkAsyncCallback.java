package org.bd.zk.callback;

import org.apache.zookeeper.AsyncCallback;

/**
 * <b>版权信息:</b> 广州智数信息科技有限公司<br>
 * <b>功能描述:</b> zookeeper连接异步回调<br>
 * <b>版本历史:</b>
 * @author  wpk | 2017年12月4日 下午5:52:50 |创建
 */
public class ZkLinkAsyncCallback implements AsyncCallback.StringCallback{

	@Override
	public void processResult(int rc, String path, Object ctx, String name) {
		System.out.println("Create path result: [" + rc + ", " + path + ", " + ctx + ", real path name: " + name);
	}

}
