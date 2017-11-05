/*Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.*/
package com.musigma.producer;

import java.util.Properties;

/**
 * @author Shruti Gupta
 *
 *         This class creates the properties object for the KAFKA producer
 *         according to the parameters it receives
 *
 */
public class ProducerProperties {

	/**
	 * @param metadataBrokerList
	 *            !!Mandatory list of brokers used for bootstrapping knowledge
	 *            about the rest of the cluster format: host1:port1,host2:port2
	 *            ... default:localhost:9092
	 * 
	 * @param producerType
	 *            !!Mandatory specifies whether the messages are sent
	 *            asynchronously (async) or synchronously (sync) default:sync
	 * 
	 * @param compressionCodec
	 *            !!Mandatory specify the compression codec for all data
	 *            generated: none , gzip, snappy. default:none
	 * 
	 * @param serializerClass
	 *            !!Mandatory message encoder
	 *            default:kafka.serializer.StringEncoder
	 * 
	 * @param partitionerClass
	 *            --Optional name of the partitioner class for partitioning
	 *            events; default partition spreads data randomly
	 * 
	 * @param compressedTopics
	 *            --Optional allow topic level compression
	 * 
	 * @param queueBufferingMaxTime
	 *            --Optional(for Async Producer only) maximum time, in
	 *            milliseconds, for buffering data on the producer queue
	 * 
	 * @param queueBufferingMaxMessages
	 *            --Optional(for Async Producer only) the maximum size of the
	 *            blocking queue for buffering on the producer
	 * 
	 * @param queueEnqueueTimeoutTime
	 *            --Optional(for Async Producer only) 0: events will be enqueued
	 *            immediately or dropped if the queue is full -ve: enqueue will
	 *            block indefinitely if the queue is full +ve: enqueue will
	 *            block up to this many milliseconds if the queue is full
	 *
	 * @param batchNumMessages
	 *            --Optional(for Async Producer only) the number of messages
	 *            batched at the producer
	 * 
	 * @return returns a Properties Object containing properties for the
	 *         Producer, to be passed to MuProducer class
	 */

	public Properties setProducerProperties(String metadataBrokerList,
			String producerType, String compressionCodec,
			String serializerClass, String partitionerClass,
			String compressedTopics, String queueBufferingMaxTime,
			String queueBufferingMaxMessages, String queueEnqueueTimeoutTime,
			String batchNumMessages) {

		Properties producerProps = new Properties();
		producerProps.put("metadata.broker.list", metadataBrokerList);
		producerProps.put("producer.type", producerType);
		producerProps.put("compression.codec", compressionCodec);
		producerProps.put("serializer.class", serializerClass);

		if (!("NULL").equalsIgnoreCase(partitionerClass))
			producerProps.put("partitioner.class", partitionerClass);

		if (!(compressedTopics.equals("NULL")))
			producerProps.put("compressed.topics", compressedTopics);

		if (!(queueBufferingMaxTime.equals("NULL")))
			producerProps.put("queue.buffering.max.ms", queueBufferingMaxTime);

		if (!(queueBufferingMaxMessages.equals("NULL")))
			producerProps.put("queue.buffering.max.messages",
					queueBufferingMaxMessages);

		if (!(queueEnqueueTimeoutTime.equals("NULL")))
			producerProps.put("queue.enqueue.timeout.ms",
					queueEnqueueTimeoutTime);

		if (!(batchNumMessages.equals("NULL")))
			producerProps.put("batch.num.messages", batchNumMessages);

		return producerProps;
	}

}
