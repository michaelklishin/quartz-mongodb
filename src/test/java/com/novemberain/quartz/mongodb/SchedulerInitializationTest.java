package com.novemberain.quartz.mongodb;

import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.net.URL;

import org.junit.Test;
import org.quartz.impl.StdSchedulerFactory;

import de.flapdoodle.embed.mongo.config.Net;

/**
 * Global tests for the initialization of the scheduler.
 * 
 * @author dregnier
 *
 */
public class SchedulerInitializationTest extends AbstractEmbeddedServerTest {
	
	@Test
	public synchronized void shoulpropertiesUrlpropertiesUrlpropertiesUrldInitializeCorrectlyWithMongoUri() throws Exception {

		final URL propertiesUrl =getClass().getResource("/SchedulerInitializationTest/quartz.properties");
		final String propertiesAbsolutePath = new File(propertiesUrl.toURI()).getAbsolutePath();
		System.setProperty(StdSchedulerFactory.PROPERTIES_FILE, propertiesAbsolutePath);
		
		assertNotNull(StdSchedulerFactory.getDefaultScheduler());

		System.clearProperty(StdSchedulerFactory.PROPERTIES_FILE);
	}

	@Override
	public Net net() {
		return new Net("localhost", 27020, false);
	}
}
