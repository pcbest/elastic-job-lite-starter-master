package com.paascloud.elastic.lite.autoconfigure;

import com.dangdang.ddframe.job.api.ElasticJob;
import com.dangdang.ddframe.job.reg.zookeeper.ZookeeperConfiguration;
import com.dangdang.ddframe.job.reg.zookeeper.ZookeeperRegistryCenter;
import com.paascloud.elastic.lite.ZookeeperRegistryProperties;
import com.paascloud.elastic.lite.annotation.ElasticJobConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * The class Registry center configuration.
 *
 * @author paascloud.net @gmail.com
 */
@Configuration
@ConditionalOnClass(ElasticJob.class)
@ConditionalOnBean(annotation = ElasticJobConfig.class)
@EnableConfigurationProperties(ZookeeperRegistryProperties.class)
public class RegistryCenterConfiguration {

	private final ZookeeperRegistryProperties regCenterProperties;

	/**
	 * Instantiates a new Registry center configuration.
	 *
	 * @param regCenterProperties the reg center properties
	 */
	@Autowired
	public RegistryCenterConfiguration(ZookeeperRegistryProperties regCenterProperties) {
		this.regCenterProperties = regCenterProperties;
	}

	/**
	 * Reg center zookeeper registry center.
	 *
	 * @return the zookeeper registry center
	 */
	@Bean(initMethod = "init")
	@ConditionalOnMissingBean
	public ZookeeperRegistryCenter regCenter() {
		ZookeeperConfiguration zookeeperConfiguration = new ZookeeperConfiguration(regCenterProperties.getZkAddressList(), regCenterProperties.getNamespace());
		zookeeperConfiguration.setBaseSleepTimeMilliseconds(regCenterProperties.getBaseSleepTimeMilliseconds());
		zookeeperConfiguration.setConnectionTimeoutMilliseconds(regCenterProperties.getConnectionTimeoutMilliseconds());
		zookeeperConfiguration.setMaxSleepTimeMilliseconds(regCenterProperties.getMaxSleepTimeMilliseconds());
		zookeeperConfiguration.setSessionTimeoutMilliseconds(regCenterProperties.getSessionTimeoutMilliseconds());
		zookeeperConfiguration.setMaxRetries(regCenterProperties.getMaxRetries());
		zookeeperConfiguration.setDigest(regCenterProperties.getDigest());
		return new ZookeeperRegistryCenter(zookeeperConfiguration);
	}

}
