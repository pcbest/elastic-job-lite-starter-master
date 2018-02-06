package com.paascloud.elastic.demo;

import com.dangdang.ddframe.job.executor.ShardingContexts;
import com.dangdang.ddframe.job.lite.api.listener.ElasticJobListener;
import lombok.extern.slf4j.Slf4j;

/**
 * The class Handle user token job listener.
 *
 * @author paascloud.net @gmail.com
 */
@Slf4j
public class DataflowJobDemoListener implements ElasticJobListener {

	/**
	 * Before job executed.
	 *
	 * @param shardingContexts the sharding contexts
	 */
	@Override
    public void beforeJobExecuted(ShardingContexts shardingContexts) {
		log.info("beforeJobExecuted - shardingContexts={}", shardingContexts);
    }

	/**
	 * After job executed.
	 *
	 * @param shardingContexts the sharding contexts
	 */
	@Override
    public void afterJobExecuted(ShardingContexts shardingContexts) {
		log.info("afterJobExecuted - shardingContexts={}", shardingContexts);
	}
}