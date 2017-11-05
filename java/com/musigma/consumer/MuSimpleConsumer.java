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
package com.musigma.consumer;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.common.InvalidConfigException;
import kafka.common.UnknownTopicOrPartitionException;
import kafka.javaapi.FetchResponse;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

/**
 * @author shrutigupta34
 *
 */
public class MuSimpleConsumer {

	/**
	 * Object representing a simple KAFKA consumer
	 */
	private SimpleConsumer consumer;
	/**
	 * ID of the client
	 */
	private String clientId;
	/**
	 * UUID key unique to each MuSimpleConsumer
	 */
	private UUID key;
	
	/**
	 * Concurrent Hash Map to store messages read by the consumer
	 */
	private static ConcurrentMap<UUID, Queue<String>> concurrentMap = new ConcurrentHashMap<UUID, Queue<String>>(100,0.75F,100);

	/**
	 * Constructor generates a random UUID
	 */
	public MuSimpleConsumer() {
		this.key = UUID.randomUUID();
	}

	/**
	 * @return key:UUID of the object
	 */
	public UUID getKey() {
		return key;
	}

	/**
	 * Creates a KAFKA simple consumer
	 * @param kafkaServerURL : URL of the KAFKA server
	 * @param kafkaServerPort :Port number of the KAFKA server
	 * @param connectionTimeOut : Connection Timeout in ms
	 * @param kafkaProducerBufferSize: Buffer size
	 * @param clientId : ID Of the client
	 */
	public void CreateSimpleConsumer(String kafkaServerURL,
			String skafkaServerPort, String sconnectionTimeOut,
			String skafkaProducerBufferSize, String clientId) {
		
		int kafkaServerPort=Integer.parseInt(skafkaServerPort);
		int connectionTimeOut=Integer.parseInt(sconnectionTimeOut);
		int kafkaProducerBufferSize=Integer.parseInt(skafkaProducerBufferSize);

		this.clientId = clientId;
		try {
			consumer = new SimpleConsumer(kafkaServerURL, kafkaServerPort,
					connectionTimeOut, kafkaProducerBufferSize, clientId);
		} 
		catch (NumberFormatException e) {
			System.out.println("Please check all consumer properties passed");
			e.printStackTrace();
		} 
		catch (IllegalArgumentException e) {
			System.out.println("Please check all consumer properties passed");
			e.printStackTrace();
		} 
		catch (org.I0Itec.zkclient.exception.ZkTimeoutException e) {
			System.out.println("Unable to connect to zookeeper server");
			e.printStackTrace();
		} 
		catch (InvalidConfigException e) {
			System.out
					.println("Invalid value set for consumer properties. Please check all consumer properties passed");
			e.printStackTrace();
		}
	}

	/**Function for consumer to read messages from topic
	 * @param topicName: Name of the topic from where to read messages
	 * @param partition: Partition Number
	 * @param Offset :Offset Number
	 * @param msgReadSize : Size of the message to be read
	 * @throws UnsupportedEncodingException
	 */
	public void receive(String topicName,String spartition, String sOffset,
			String smsgReadSize) throws UnsupportedEncodingException {
		try {
			
			int partition=Integer.parseInt(spartition);
			int Offset=Integer.parseInt(sOffset);
			int msgReadSize=Integer.parseInt(smsgReadSize);
			
			Queue<String> msgs = new LinkedList<String>();
			long readOffset = (long) Offset;
			
			FetchRequest req = new FetchRequestBuilder().clientId(clientId)
					.addFetch(topicName, partition, readOffset, msgReadSize)
					.build();
			FetchResponse fetchResponse = consumer.fetch(req);
			ByteBufferMessageSet toRead = fetchResponse.messageSet(topicName,
					partition);
			
			for (MessageAndOffset messageAndOffset : toRead) {
				ByteBuffer payload = messageAndOffset.message().payload();
				byte[] bytes = new byte[payload.limit()];
				payload.get(bytes);
				String msg = (new String(bytes, "UTF-8"));
				msgs.add(msg);
				concurrentMap.put(key, msgs);

			}
		} 
		catch(UnsupportedEncodingException e){
			System.out.println("Unsupported Encoding");
			e.printStackTrace();
		}
		catch (UnknownTopicOrPartitionException e) {

			System.out.println("Unknown topic or partition");
			e.printStackTrace();

		} 
		catch (IllegalArgumentException e) {
			System.out.println("Please check all parameters passed");
			e.printStackTrace();
		} 
		catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 ** @return: Returns messages from the object's queue one at a time
	 */
	public String read() {
		
		UUID retrieveKey = this.getKey();
		Queue<String> readMessages = concurrentMap
				.get(retrieveKey);
		
		if (readMessages.isEmpty())
			return "";
		else
			return (readMessages.poll());
	}

	/**
	 * Close the consumer
	 */
	public void close() {
		consumer.close();
	}

}