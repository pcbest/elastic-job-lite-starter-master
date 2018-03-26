package com.paascloud.elastic.lite.job;

import com.dangdang.ddframe.job.api.ShardingContext;
import com.dangdang.ddframe.job.api.dataflow.DataflowJob;
import com.google.common.base.Splitter;
import com.paascloud.elastic.lite.GlobalConstant;
import com.paascloud.elastic.lite.JobParameter;
import lombok.extern.slf4j.Slf4j;
import org.modelmapper.ModelMapper;

import java.util.List;
import java.util.Map;

/**
 * The class Abstract base dataflow job.
 *
 * @param <T> the type parameter
 *
 * @author paascloud.net @gmail.com
 */
@Slf4j
public abstract class AbstractBaseDataflowJob<T> implements DataflowJob<T> {

	/**
	 * Fetch job data list.
	 *
	 * @param jobTaskParameter the job task parameter
	 *
	 * @return the list
	 */
	protected abstract List<T> fetchJobData(JobParameter jobTaskParameter);

	/**
	 * Process job data.
	 *
	 * @param taskList the task list
	 */
	protected abstract void processJobData(List<T> taskList);

	/**
	 * Fetch data list.
	 *
	 * @param shardingContext the sharding context
	 *
	 * @return the list
	 */
	@Override
	public List<T> fetchData(ShardingContext shardingContext) {
		String jobName = shardingContext.getJobName();
		int shardingItem = shardingContext.getShardingItem();
		int shardingTotalCount = shardingContext.getShardingTotalCount();
		String taskId = shardingContext.getTaskId();
		String parameter = shardingContext.getJobParameter();
		final Map<String, String> map = Splitter.on(GlobalConstant.Symbol.COMMA).withKeyValueSeparator(GlobalConstant.Symbol.SIGN).split(parameter);
		JobParameter jobTaskParameter = new ModelMapper().map(map, JobParameter.class);
		jobTaskParameter.setShardingItem(shardingItem).setShardingTotalCount(shardingTotalCount);
		log.info("扫描worker任务列表开始,jobName={}, shardingItem={}, shardingTotalCount={}, taskId={}", jobName, shardingItem, shardingTotalCount, taskId);
		long startTimestamp = System.currentTimeMillis();
		List<T> taskLst = fetchJobData(jobTaskParameter);
		int taskNo = taskLst != null ? taskLst.size() : 0;
		long endTimestamp = System.currentTimeMillis();
		log.info("扫描worker任务列表结束共计加载[{}]个任务, 耗时=[{}]",taskNo, (endTimestamp - startTimestamp));
		return taskLst;
	}

	/**
	 * Process data.
	 *
	 * @param shardingContext the sharding context
	 * @param workerTask      the worker task
	 */
	@Override
	public void processData(ShardingContext shardingContext, List<T> workerTask) {
		log.info("任务[" + workerTask.get(0).getClass().getName() + "]开始执行...");
		long startTimestamp = System.currentTimeMillis();
		processJobData(workerTask);
		long endTimestamp = System.currentTimeMillis();
		log.info("任务[" + workerTask.get(0).getClass().getName() + "]执行完毕:耗时=[{}]", (endTimestamp - startTimestamp));
	}
}
