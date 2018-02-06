package com.paascloud.elastic.demo;

import com.paascloud.elastic.lite.annotation.ElasticJobConfig;
import com.paascloud.elastic.lite.job.AbstractBaseDataflowJob;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * The class Dataflow job demo.
 *
 * @param <Foo> the type parameter
 *
 * @author paascloud.net @gmail.com
 */
@ElasticJobConfig(cron = "0/3 * * * * ? ", shardingTotalCount = 2, jobParameter = "fetchNum=200,taskType=SENDING_MESSAGE")
@Component
@Slf4j
public class DataflowJobDemo<Foo> extends AbstractBaseDataflowJob {
	/**
	 * Fetch job data list.
	 *
	 * @param shardingItem the sharding item
	 * @param fetchNum     the fetch num
	 * @param jobName      the job name
	 *
	 * @return the list
	 */
	@Override
	protected List<Foo> fetchJobData(int shardingItem, int fetchNum, String jobName) {
		log.info("shardingItem={}, fetchNum={}, jobName={}", shardingItem, fetchNum, jobName);
		return null;
	}

	/**
	 * Process job data.
	 *
	 * @param taskList the task list
	 * @param jobName  the job name
	 */
	@Override
	protected void processJobData(List taskList, String jobName) {

	}
}
