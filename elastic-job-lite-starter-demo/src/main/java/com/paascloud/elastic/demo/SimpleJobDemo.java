package com.paascloud.elastic.demo;

import com.dangdang.ddframe.job.api.ShardingContext;
import com.dangdang.ddframe.job.api.simple.SimpleJob;
import com.paascloud.elastic.lite.annotation.ElasticJobConfig;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * The class Simple job demo.
 *
 * @author paascloud.net @gmail.com
 */
@ElasticJobConfig(cron = "0/4 * * * * ?", shardingTotalCount = 1, shardingItemParameters = "0=Beijing,1=Shanghai,2=Guangzhou")
@Component
public class SimpleJobDemo implements SimpleJob {
	/**
	 * Execute.
	 *
	 * @param shardingContext the sharding context
	 */
	@Override
	public void execute(ShardingContext shardingContext) {
		System.out.println(String.format("Item: %s | Time: %s | Thread: %s | %s",
				shardingContext.getShardingItem(), new SimpleDateFormat("HH:mm:ss").format(new Date()), Thread.currentThread().getId(), "SimpleJob FETCH"));
	}
}
