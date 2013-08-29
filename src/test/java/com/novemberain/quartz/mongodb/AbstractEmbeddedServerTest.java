package com.novemberain.quartz.mongodb;

import org.junit.After;
import org.junit.Before;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;

/**
 * Abstract unit test class for 'real situation' unit tests - with an embedded MongoDB server.
 *
 * @author dregnier
 *
 */
public abstract class AbstractEmbeddedServerTest {

	private MongodExecutable mongodExecutable;
	private MongoClient mongoClient;

	@Before
	public void setup() throws Exception {
		final MongodStarter runtime = MongodStarter.getDefaultInstance();
		mongodExecutable = runtime.prepare(new MongodConfigBuilder().version(Version.Main.PRODUCTION).net(net()).build());
		final MongodProcess mongodProcess = mongodExecutable.start();
		mongoClient = new MongoClient(new ServerAddress(mongodProcess.getConfig().net().getServerAddress(),
				mongodProcess.getConfig().net().getPort()));
	}
	
	public abstract Net net();

	@After
	public void teardown() {
		mongodExecutable.stop();
	}

	protected MongoClient getMongoClient() {
		return mongoClient;
	}
}
