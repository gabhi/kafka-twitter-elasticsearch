package com.nflabs.peloton2.kafka.elasticsearch;

import java.util.concurrent.atomic.AtomicInteger;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.indices.IndexAlreadyExistsException;

import twitter4j.HashtagEntity;
import twitter4j.Status;
import twitter4j.URLEntity;
import twitter4j.UserMentionEntity;
import twitter4j.json.DataObjectFactory;

@SuppressWarnings("rawtypes")
public class Consumer implements Runnable {

	private KafkaStream m_stream;
	private int m_threadNumber;
	private final Context m_context;

	private final Client client;
	private volatile BulkRequestBuilder currentRequest;
	private final AtomicInteger onGoingBulks = new AtomicInteger();
	private final int bulkSize;
	private final int dropThreshold;

	public Consumer(KafkaStream a_stream, int a_threadNumber, Context context) {
		m_threadNumber = a_threadNumber;
		m_stream = a_stream;
		m_context = context;
		bulkSize = 100;
		dropThreshold = 10;

		Settings settings = ImmutableSettings.settingsBuilder().put("node.name", context.getString(TwitterConsumerConstant.ES_NODE))
																.put("cluster.name", context.getString(TwitterConsumerConstant.ES_CLUSTER))
																.build();
		client = new TransportClient(settings);
		String[] hosts = (context.getString(TwitterConsumerConstant.ES_HOSTS)).split(",");
		for (String host : hosts) {
			String[] hostPort = host.split(":");
			((TransportClient) client).addTransportAddress(new InetSocketTransportAddress(hostPort[0], Integer.parseInt(hostPort[1])));
		}
		currentRequest = client.prepareBulk();

		try{
			String mapping = XContentFactory.jsonBuilder().startObject().startObject(context.getString(TwitterConsumerConstant.ES_TYPE)).startObject("properties")
                    .startObject("location").field("type", "geo_point").endObject()
                    .startObject("user").startObject("properties").startObject("screen_name").field("type", "string").field("index", "not_analyzed").endObject().endObject().endObject()
                    .startObject("mention").startObject("properties").startObject("screen_name").field("type", "string").field("index", "not_analyzed").endObject().endObject().endObject()
                    .startObject("in_reply").startObject("properties").startObject("user_screen_name").field("type", "string").field("index", "not_analyzed").endObject().endObject().endObject()
                    .endObject().endObject().endObject().string();
            client.admin().indices().prepareCreate(context.getString(TwitterConsumerConstant.ES_INDEX)).addMapping(context.getString(TwitterConsumerConstant.ES_TYPE), mapping).execute().actionGet();
		}
		catch (Exception e) {
            if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
                // that's fine
            } else if (ExceptionsHelper.unwrapCause(e) instanceof ClusterBlockException) {
                /** Not covered yet **/
            } else {
            	System.out.println("failed to create index [{}]...");
                return;
            }
        }
	}

	private void processBulkIfNeeded(boolean force) {
        if (force || currentRequest.numberOfActions() >= bulkSize) {
            // execute the bulk operation
            int currentOnGoingBulks = onGoingBulks.incrementAndGet();
            if (currentOnGoingBulks > dropThreshold) {
                onGoingBulks.decrementAndGet();

            } else {
                try {
                    currentRequest.execute(new ActionListener<BulkResponse>() {
                        @Override
                        public void onResponse(BulkResponse bulkResponse) {
                            onGoingBulks.decrementAndGet();
                        }

                        @Override
                        public void onFailure(Throwable e) {
                            onGoingBulks.decrementAndGet();
                            System.out.println("failed to execute bulk");
                        }
                    });
                } catch (Exception e) {
                    onGoingBulks.decrementAndGet();
                    System.out.println("failed to process bulk " +  e.getMessage());
                }
            }
            currentRequest = client.prepareBulk();
        }
    }

	@Override
	public void run() {
		ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
		while (it.hasNext()) {
			try {
				Status status = DataObjectFactory.createStatus(new String(it.next().message(), "UTF-8"));
				XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
                builder.field("text", status.getText());
                builder.field("created_at", status.getCreatedAt());
                builder.field("source", status.getSource());
                builder.field("truncated", status.isTruncated());

                if (status.getUserMentionEntities() != null) {
                    builder.startArray("mention");
                    for (UserMentionEntity user : status.getUserMentionEntities()) {
                        builder.startObject();
                        builder.field("id", user.getId());
                        builder.field("name", user.getName());
                        builder.field("screen_name", user.getScreenName());
                        builder.field("start", user.getStart());
                        builder.field("end", user.getEnd());
                        builder.endObject();
                    }
                    builder.endArray();
                }

                if (status.getRetweetCount() != -1) {
                    builder.field("retweet_count", status.getRetweetCount());
                }

                if (status.isRetweet() && status.getRetweetedStatus() != null) {
                    builder.startObject("retweet");
                    builder.field("id", status.getRetweetedStatus().getId());
                    if (status.getRetweetedStatus().getUser() != null) {
                        builder.field("user_id", status.getRetweetedStatus().getUser().getId());
                        builder.field("user_screen_name", status.getRetweetedStatus().getUser().getScreenName());
                        if (status.getRetweetedStatus().getRetweetCount() != -1) {
                            builder.field("retweet_count", status.getRetweetedStatus().getRetweetCount());
                        }
                    }
                    builder.endObject();
                }

                if (status.getInReplyToStatusId() != -1) {
                    builder.startObject("in_reply");
                    builder.field("status", status.getInReplyToStatusId());
                    if (status.getInReplyToUserId() != -1) {
                        builder.field("user_id", status.getInReplyToUserId());
                        builder.field("user_screen_name", status.getInReplyToScreenName());
                    }
                    builder.endObject();
                }

                if (status.getHashtagEntities() != null) {
                    builder.startArray("hashtag");
                    for (HashtagEntity hashtag : status.getHashtagEntities()) {
                        builder.startObject();
                        builder.field("text", hashtag.getText());
                        builder.field("start", hashtag.getStart());
                        builder.field("end", hashtag.getEnd());
                        builder.endObject();
                    }
                    builder.endArray();
                }
                if (status.getContributors() != null && status.getContributors().length > 0) {
                    builder.array("contributor", status.getContributors());
                }
                if (status.getGeoLocation() != null) {
                    builder.startObject("location");
                    builder.field("lat", status.getGeoLocation().getLatitude());
                    builder.field("lon", status.getGeoLocation().getLongitude());
                    builder.endObject();
                }
                if (status.getPlace() != null) {
                    builder.startObject("place");
                    builder.field("id", status.getPlace().getId());
                    builder.field("name", status.getPlace().getName());
                    builder.field("type", status.getPlace().getPlaceType());
                    builder.field("full_name", status.getPlace().getFullName());
                    builder.field("street_address", status.getPlace().getStreetAddress());
                    builder.field("country", status.getPlace().getCountry());
                    builder.field("country_code", status.getPlace().getCountryCode());
                    builder.field("url", status.getPlace().getURL());
                    builder.endObject();
                }
                if (status.getURLEntities() != null) {
                    builder.startArray("link");
                    for (URLEntity url : status.getURLEntities()) {
                        if (url != null) {
                            builder.startObject();
                            if (url.getURL() != null) {
                                builder.field("url", url.getURL());
                            }
                            if (url.getDisplayURL() != null) {
                                builder.field("display_url", url.getDisplayURL());
                            }
                            if (url.getExpandedURL() != null) {
                                builder.field("expand_url", url.getExpandedURL());
                            }
                            builder.field("start", url.getStart());
                            builder.field("end", url.getEnd());
                            builder.endObject();
                        }
                    }
                    builder.endArray();
                }

                builder.startObject("user");
                builder.field("id", status.getUser().getId());
                builder.field("name", status.getUser().getName());
                builder.field("screen_name", status.getUser().getScreenName());
                builder.field("location", status.getUser().getLocation());
                builder.field("description", status.getUser().getDescription());
                builder.field("profile_image_url", status.getUser().getProfileImageURL());
                builder.field("profile_image_url_https", status.getUser().getProfileImageURLHttps());

                builder.endObject();

                builder.endObject();
                System.out.println("get Tweet");
                currentRequest.add(Requests.indexRequest(m_context.getString(TwitterConsumerConstant.ES_INDEX)).type(m_context.getString(TwitterConsumerConstant.ES_TYPE)).id(Long.toString(status.getId())).create(true).source(builder));
                processBulkIfNeeded(false);
			} catch (Exception e) {
				System.out.println("failed to construct index request "+ e.getMessage());
            }

		}
		System.out.println("Shutting down Thread: " + m_threadNumber);
	}
}
