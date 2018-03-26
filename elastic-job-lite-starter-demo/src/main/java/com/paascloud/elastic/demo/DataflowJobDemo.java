package com.paascloud.elastic.demo;

import com.google.common.collect.Lists;
import com.paascloud.elastic.lite.JobParameter;
import com.paascloud.elastic.lite.annotation.ElasticJobConfig;
import com.paascloud.elastic.lite.job.AbstractBaseDataflowJob;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * The class Dataflow job demo.
 *
 * @author paascloud.net @gmail.com
 */
@ElasticJobConfig(cron = "0/50 * * * * ? ", listener = DataflowJobDemoListener.class, jobParameter = "fetchNum=200,taskType=SENDING_MESSAGE")
@Component
@Slf4j
public class DataflowJobDemo extends AbstractBaseDataflowJob<Foo> {
	private List<Foo> list = Lists.newArrayList(new Foo(1L), new Foo(2L));

	@Override
	protected List<Foo> fetchJobData(final JobParameter jobTaskParameter) {
		log.info("fetchJobData - jobTaskParameter={}", jobTaskParameter);
		return list;
	}

	@Override
	protected void processJobData(final List<Foo> taskList) {
		log.info("processJobData - taskList={}", taskList);
		list.clear();
	}
}
