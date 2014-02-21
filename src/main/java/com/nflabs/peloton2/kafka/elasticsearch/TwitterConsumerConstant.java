package com.nflabs.peloton2.kafka.elasticsearch;

public class TwitterConsumerConstant {
	public static final String ZOOKEEPERS = "zookeeper.connect";
	public static final String GROUP_ID = "consumer.group.id";
	public static final String ZOOKEEPER_SYNC = "zookeeper.sync.time.ms";
	public static final String ZOOKEEPER_TIMEOUT = "zookeeper.session.timeout.ms";
	public static final String AUTO_COMMIT_INTERVAL = "consumer.auto.commit.interval.ms";
	public static final String TOPIC = "consumer.topic";
	public static final String THREADS = "consumer.thread.number";
	public static final String AUTO_OFFSET_RESET = "consumer.auto.offset.reset";

	public static final String ES_NODE = "elasticsearch.node.name";
	public static final String ES_CLUSTER = "elasticsearch.cluster.name";
	public static final String ES_INDEX = "elasticsearch.index";
	public static final String ES_TYPE = "elasticsearch.type";
	public static final String ES_HOSTS = "elasticsearch.hosts";
	public static final String ES_ENCODING = "elasticsearch.encoding";
}
