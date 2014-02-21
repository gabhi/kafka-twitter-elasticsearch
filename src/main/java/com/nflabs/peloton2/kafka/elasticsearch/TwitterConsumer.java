package com.nflabs.peloton2.kafka.elasticsearch;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TwitterConsumer {

	private static final Logger logger = LoggerFactory.getLogger(TwitterConsumer.class);

	private final ConsumerConnector consumer;
    private final String topic;
    private  ExecutorService executor;

    public TwitterConsumer(String zookeeper, String groupId, String topic, Context context) {
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig(zookeeper, groupId, context));
        this.topic = topic;
    }

    public void shutdown() {
        if (consumer != null) consumer.shutdown();
        if (executor != null) executor.shutdown();
    }

    private static ConsumerConfig createConsumerConfig(String zookeeper, String groupId, Context context) {
        Properties props = new Properties();
        props.put("zookeeper.connect", zookeeper);
        props.put("group.id", groupId);
        props.put("zookeeper.session.timeout.ms", context.getString(TwitterConsumerConstant.ZOOKEEPER_TIMEOUT));
        props.put("zookeeper.sync.time.ms", context.getString(TwitterConsumerConstant.ZOOKEEPER_SYNC));
        props.put("auto.commit.interval.ms", context.getString(TwitterConsumerConstant.AUTO_COMMIT_INTERVAL));

        return new ConsumerConfig(props);
    }

    public void run(int numThreads, Context context) {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(numThreads));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        /** Start the threads **/
        executor = Executors.newFixedThreadPool(numThreads);

        /** kafka consumer object **/
        int threadNumber = 0;
        for (final KafkaStream<?, ?> stream : streams) {
            executor.submit(new Consumer(stream, threadNumber, context));
            threadNumber++;
        }
    }

	public static void main(String[] args) {
		try {
			/** Getting the config **/
			Context context = new Context(args[0]);
			String zooKeeper = context.getString(TwitterConsumerConstant.ZOOKEEPERS);
			String groupId = context.getString(TwitterConsumerConstant.GROUP_ID);
			String topic = context.getString(TwitterConsumerConstant.TOPIC);
			int threads = context.getInt(TwitterConsumerConstant.THREADS);

			TwitterConsumer EsComsumer = new TwitterConsumer(zooKeeper,groupId, topic, context);
			EsComsumer.run(threads, context);
		} catch (Exception e) {
			logger.error("{} " + e.getMessage());
			e.printStackTrace();
		}
    }

}
