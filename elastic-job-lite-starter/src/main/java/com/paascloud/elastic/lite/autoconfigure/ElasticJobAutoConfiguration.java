package com.paascloud.elastic.lite.autoconfigure;

import com.dangdang.ddframe.job.api.ElasticJob;
import com.dangdang.ddframe.job.api.JobType;
import com.dangdang.ddframe.job.api.dataflow.DataflowJob;
import com.dangdang.ddframe.job.api.script.ScriptJob;
import com.dangdang.ddframe.job.api.simple.SimpleJob;
import com.dangdang.ddframe.job.config.JobCoreConfiguration;
import com.dangdang.ddframe.job.config.JobTypeConfiguration;
import com.dangdang.ddframe.job.config.dataflow.DataflowJobConfiguration;
import com.dangdang.ddframe.job.config.script.ScriptJobConfiguration;
import com.dangdang.ddframe.job.config.simple.SimpleJobConfiguration;
import com.dangdang.ddframe.job.event.rdb.JobEventRdbConfiguration;
import com.dangdang.ddframe.job.executor.handler.JobProperties;
import com.dangdang.ddframe.job.lite.api.listener.AbstractDistributeOnceElasticJobListener;
import com.dangdang.ddframe.job.lite.api.listener.ElasticJobListener;
import com.dangdang.ddframe.job.lite.config.LiteJobConfiguration;
import com.dangdang.ddframe.job.lite.spring.api.SpringJobScheduler;
import com.dangdang.ddframe.job.reg.zookeeper.ZookeeperRegistryCenter;
import com.paascloud.elastic.lite.annotation.ElasticJobConfig;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The class Elastic job auto configuration.
 *
 * @author paascloud.net @gmail.com
 */
@Configuration
@ConditionalOnExpression("'${elaticjob.zookeeper.server-lists}'.length() > 0")
public class ElasticJobAutoConfiguration {

	@Resource
	private ZookeeperRegistryCenter regCenter;

	@Resource
	private ApplicationContext applicationContext;

	/**
	 * Init.
	 */
	@PostConstruct
	public void init() {
		//获取作业任务
		Map<String, ElasticJob> elasticJobMap = applicationContext.getBeansOfType(ElasticJob.class);
		//循环解析任务
		for (ElasticJob elasticJob : elasticJobMap.values()) {
			Class<? extends ElasticJob> jobClass = elasticJob.getClass();
			//获取作业任务注解配置
			ElasticJobConfig elasticJobConfig = jobClass.getAnnotation(ElasticJobConfig.class);
			//获取Lite作业配置
			LiteJobConfiguration liteJobConfiguration = getLiteJobConfiguration(getJobType(elasticJob), jobClass, elasticJobConfig);
			//获取作业事件追踪的数据源配置
			JobEventRdbConfiguration jobEventRdbConfiguration = getJobEventRdbConfiguration(elasticJobConfig.eventTraceRdbDataSource());
			//获取作业监听器
			ElasticJobListener[] elasticJobListeners = createElasticJobListeners(elasticJobConfig);
			elasticJobListeners = Objects.isNull(elasticJobListeners) ? new ElasticJobListener[0] : elasticJobListeners;
			//注册作业
			if (Objects.isNull(jobEventRdbConfiguration)) {
				new SpringJobScheduler(elasticJob, regCenter, liteJobConfiguration, elasticJobListeners).init();
			} else {
				new SpringJobScheduler(elasticJob, regCenter, liteJobConfiguration, jobEventRdbConfiguration, elasticJobListeners).init();
			}
		}
	}

	/**
	 * 获取作业事件追踪的数据源配置
	 *
	 * @param eventTraceRdbDataSource 作业事件追踪的数据源Bean引用
	 * @return JobEventRdbConfiguration
	 */
	private JobEventRdbConfiguration getJobEventRdbConfiguration(String eventTraceRdbDataSource) {
		if (StringUtils.isBlank(eventTraceRdbDataSource)) {
			return null;
		}
		if (!applicationContext.containsBean(eventTraceRdbDataSource)) {
			throw new RuntimeException("not exist datasource [" + eventTraceRdbDataSource + "] !");
		}
		DataSource dataSource = (DataSource) applicationContext.getBean(eventTraceRdbDataSource);
		return new JobEventRdbConfiguration(dataSource);
	}

	/**
	 * 获取作业任务类型
	 *
	 * @param elasticJob 作业任务
	 * @return JobType
	 */
	private JobType getJobType(ElasticJob elasticJob) {
		if (elasticJob instanceof SimpleJob) {
			return JobType.SIMPLE;
		} else if (elasticJob instanceof DataflowJob) {
			return JobType.DATAFLOW;
		} else if (elasticJob instanceof ScriptJob) {
			return JobType.SCRIPT;
		} else {
			throw new RuntimeException("unknown JobType [" + elasticJob.getClass() + "]!");
		}
	}

	/**
	 * 构建任务核心配置
	 *
	 * @param jobName          任务执行名称
	 * @param elasticJobConfig 任务配置
	 * @return JobCoreConfiguration
	 */
	private JobCoreConfiguration getJobCoreConfiguration(String jobName, ElasticJobConfig elasticJobConfig) {
		JobCoreConfiguration.Builder builder = JobCoreConfiguration.newBuilder(jobName, elasticJobConfig.cron(), elasticJobConfig.shardingTotalCount())
				.shardingItemParameters(elasticJobConfig.shardingItemParameters())
				.jobParameter(elasticJobConfig.jobParameter())
				.failover(elasticJobConfig.failover())
				.misfire(elasticJobConfig.misfire())
				.description(elasticJobConfig.description());
		if (StringUtils.isNotBlank(elasticJobConfig.jobExceptionHandler())) {
			builder.jobProperties(JobProperties.JobPropertiesEnum.JOB_EXCEPTION_HANDLER.getKey(), elasticJobConfig.jobExceptionHandler());
		}
		if (StringUtils.isNotBlank(elasticJobConfig.executorServiceHandler())) {
			builder.jobProperties(JobProperties.JobPropertiesEnum.EXECUTOR_SERVICE_HANDLER.getKey(), elasticJobConfig.executorServiceHandler());
		}
		return builder.build();
	}

	/**
	 * 构建Lite作业
	 *
	 * @param jobType          任务类型
	 * @param jobClass         任务执行类
	 * @param elasticJobConfig 任务配置
	 * @return LiteJobConfiguration
	 */
	private LiteJobConfiguration getLiteJobConfiguration(final JobType jobType, final Class<? extends ElasticJob> jobClass, ElasticJobConfig elasticJobConfig) {

		//构建核心配置
		JobCoreConfiguration jobCoreConfiguration = getJobCoreConfiguration(jobClass.getName(), elasticJobConfig);

		//构建任务类型配置
		JobTypeConfiguration jobTypeConfiguration = getJobTypeConfiguration(jobCoreConfiguration, jobType, jobClass.getCanonicalName(),
				elasticJobConfig.streamingProcess(), elasticJobConfig.scriptCommandLine());

		//构建Lite作业
		return LiteJobConfiguration.newBuilder(Objects.requireNonNull(jobTypeConfiguration))
				.monitorExecution(elasticJobConfig.monitorExecution())
				.monitorPort(elasticJobConfig.monitorPort())
				.maxTimeDiffSeconds(elasticJobConfig.maxTimeDiffSeconds())
				.jobShardingStrategyClass(elasticJobConfig.jobShardingStrategyClass())
				.reconcileIntervalMinutes(elasticJobConfig.reconcileIntervalMinutes())
				.disabled(elasticJobConfig.disabled())
				.overwrite(elasticJobConfig.overwrite()).build();

	}

	/**
	 * 获取任务类型配置
	 *
	 * @param jobCoreConfiguration 作业核心配置
	 * @param jobType              作业类型
	 * @param jobClass             作业类
	 * @param streamingProcess     是否流式处理数据
	 * @param scriptCommandLine    脚本型作业执行命令行
	 * @return JobTypeConfiguration
	 */
	private JobTypeConfiguration getJobTypeConfiguration(JobCoreConfiguration jobCoreConfiguration, JobType jobType,
	                                                     String jobClass, boolean streamingProcess, String scriptCommandLine) {
		switch (jobType) {
			case DATAFLOW:
				return new DataflowJobConfiguration(jobCoreConfiguration, jobClass, streamingProcess);
			case SCRIPT:
				return new ScriptJobConfiguration(jobCoreConfiguration, scriptCommandLine);
			case SIMPLE:
			default:
				return new SimpleJobConfiguration(jobCoreConfiguration, jobClass);
		}
	}

	/**
	 * 获取监听器
	 *
	 * @param elasticJobConfig 任务配置
	 * @return ElasticJobListener[]
	 */
	private ElasticJobListener[] createElasticJobListeners(ElasticJobConfig elasticJobConfig) {
		List<ElasticJobListener> elasticJobListeners = new ArrayList<>(2);

		//注册每台作业节点均执行的监听
		ElasticJobListener elasticJobListener = createElasticJobListener(elasticJobConfig.listener());
		if (Objects.nonNull(elasticJobListener)) {
			elasticJobListeners.add(elasticJobListener);
		}

		//注册分布式监听者
		AbstractDistributeOnceElasticJobListener distributedListener = createAbstractDistributeOnceElasticJobListener(elasticJobConfig.distributedListener(),
				elasticJobConfig.startedTimeoutMilliseconds(), elasticJobConfig.completedTimeoutMilliseconds());
		if (Objects.nonNull(distributedListener)) {
			elasticJobListeners.add(distributedListener);
		}

		if (CollectionUtils.isEmpty(elasticJobListeners)) {
			return null;
		}

		//集合转数组
		ElasticJobListener[] elasticJobListenerArray = new ElasticJobListener[elasticJobListeners.size()];
		for (int i = 0; i < elasticJobListeners.size(); i++) {
			elasticJobListenerArray[i] = elasticJobListeners.get(i);
		}
		return elasticJobListenerArray;
	}

	/**
	 * 创建每台作业节点均执行的监听
	 *
	 * @param listener 监听者
	 * @return ElasticJobListener
	 */
	private ElasticJobListener createElasticJobListener(Class<? extends ElasticJobListener> listener) {
		//判断是否配置了监听者
		if (listener.isInterface()) {
			return null;
		}
		//判断监听者是否已经在spring容器中存在
		if (applicationContext.containsBean(listener.getSimpleName())) {
			return applicationContext.getBean(listener.getSimpleName(), ElasticJobListener.class);
		}
		//不存在则创建并注册到Spring容器中
		return registerElasticJobListener(listener);
	}

	/**
	 * 创建分布式监听者到spring容器
	 *
	 * @param distributedListener          监听者
	 * @param startedTimeoutMilliseconds   最后一个作业执行前的执行方法的超时时间 单位：毫秒
	 * @param completedTimeoutMilliseconds 最后一个作业执行后的执行方法的超时时间 单位：毫秒
	 * @return AbstractDistributeOnceElasticJobListener
	 */
	private AbstractDistributeOnceElasticJobListener createAbstractDistributeOnceElasticJobListener(Class<? extends AbstractDistributeOnceElasticJobListener> distributedListener,
	                                                                                               long startedTimeoutMilliseconds,
	                                                                                               long completedTimeoutMilliseconds) {
		//判断是否配置了监听者
		if (Objects.equals(distributedListener, AbstractDistributeOnceElasticJobListener.class)) {
			return null;
		}
		//判断监听者是否已经在spring容器中存在
		if (applicationContext.containsBean(distributedListener.getSimpleName())) {
			return applicationContext.getBean(distributedListener.getSimpleName(), AbstractDistributeOnceElasticJobListener.class);
		}
		//不存在则创建并注册到Spring容器中
		return registerAbstractDistributeOnceElasticJobListener(distributedListener, startedTimeoutMilliseconds, completedTimeoutMilliseconds);
	}

	/**
	 * 注册每台作业节点均执行的监听到spring容器
	 *
	 * @param listener 监听者
	 * @return ElasticJobListener
	 */
	private ElasticJobListener registerElasticJobListener(Class<? extends ElasticJobListener> listener) {
		BeanDefinitionBuilder beanDefinitionBuilder = BeanDefinitionBuilder.rootBeanDefinition(listener);
		beanDefinitionBuilder.setScope(BeanDefinition.SCOPE_PROTOTYPE);
		getDefaultListableBeanFactory().registerBeanDefinition(listener.getSimpleName(), beanDefinitionBuilder.getBeanDefinition());
		return applicationContext.getBean(listener.getSimpleName(), listener);
	}

	/**
	 * 注册分布式监听者到spring容器
	 *
	 * @param distributedListener          监听者
	 * @param startedTimeoutMilliseconds   最后一个作业执行前的执行方法的超时时间 单位：毫秒
	 * @param completedTimeoutMilliseconds 最后一个作业执行后的执行方法的超时时间 单位：毫秒
	 * @return AbstractDistributeOnceElasticJobListener
	 */
	private AbstractDistributeOnceElasticJobListener registerAbstractDistributeOnceElasticJobListener(Class<? extends AbstractDistributeOnceElasticJobListener> distributedListener,
	                                                                                                  long startedTimeoutMilliseconds,
	                                                                                                  long completedTimeoutMilliseconds) {
		BeanDefinitionBuilder beanDefinitionBuilder = BeanDefinitionBuilder.rootBeanDefinition(distributedListener);
		beanDefinitionBuilder.setScope(BeanDefinition.SCOPE_PROTOTYPE);
		beanDefinitionBuilder.addConstructorArgValue(startedTimeoutMilliseconds);
		beanDefinitionBuilder.addConstructorArgValue(completedTimeoutMilliseconds);
		getDefaultListableBeanFactory().registerBeanDefinition(distributedListener.getSimpleName(), beanDefinitionBuilder.getBeanDefinition());
		return applicationContext.getBean(distributedListener.getSimpleName(), distributedListener);
	}

	/**
	 * 获取beanFactory
	 *
	 * @return DefaultListableBeanFactory
	 */
	private DefaultListableBeanFactory getDefaultListableBeanFactory() {
		return (DefaultListableBeanFactory) ((ConfigurableApplicationContext) applicationContext).getBeanFactory();
	}

}
