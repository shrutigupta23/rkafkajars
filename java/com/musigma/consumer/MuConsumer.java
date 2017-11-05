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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import kafka.common.InvalidConfigException;
import kafka.common.UnknownTopicOrPartitionException;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

/**
 * @author Shruti Gupta This class implements the High Level KAFKA Consumer API
 *         Every instance of MuHighConsumer is associated with a KAFKA consumer
 *
 */

public class MuConsumer {

	private List<String> messagesList;
	private ConsumerIterator<byte[], byte[]> it;
	/**
	 * ConsumerConnector to create connection to the Consumer
	 */
	private ConsumerConnector consumerConnector;

	/**
	 * Function sets the properties and initializes the consumer connector
	 * 
	 * @param zookeeperConnect
	 *            !!Mandatory:Zookeeper connection string comma separated
	 *            host:port pairs, each corresponding to a zk server. e.g.
	 *            "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002" default:
	 *            "127.0.0.1:2181"
	 * @param groupId
	 *            !!Mandatory:consumer group id default:test-consumer-group
	 * @param zookeeperConnectionTimeoutMs
	 *            !!Mandatory:timeout in ms for connecting to zookeeper
	 *            default:100000
	 * @param consumerTimeoutMs
	 *            !!Mandatory:Throw a timeout exception to the consumer if no
	 *            message is available for consumption after the specified
	 *            interval default:1000
	 * @param autoCommitEnable
	 *            --Optional:default:true If true, periodically commit to
	 *            ZooKeeper the offset of messages already fetched by the
	 *            consumer. This committed offset will be used when the process
	 *            fails as the position from which the new consumer will begin.
	 * @param autoCommitIntervalMs
	 *            --Optional:default:60*1000 The frequency in ms that the
	 *            consumer offsets are committed to zookeeper.
	 * @param autoOffsetReset
	 *            --Optional:default:largest * smallest : automatically reset
	 *            the offset to the smallest offset largest : automatically
	 *            reset the offset to the largest offset anything else: throw
	 *            exception to the consumer
	 */

	public void CreateConsumer(String zookeeperConnect, String groupId,
			String zookeeperConnectionTimeoutMs, String consumerTimeoutMs,
			String autoCommitEnable, String autoCommitIntervalMs,
			String autoOffsetReset) {

		try {
			Properties properties = new Properties();

			properties.put("zookeeper.connect", zookeeperConnect);
			properties.put("group.id", groupId);
			properties.put("zookeeper.connection.timeout.ms",
					zookeeperConnectionTimeoutMs);
			properties.put("consumer.timeout.ms", consumerTimeoutMs);


			if (!(("NULL").equalsIgnoreCase(autoCommitEnable))){
				properties.put("auto.commit.enable", autoCommitEnable);
			}

			if (!(("NULL").equalsIgnoreCase(autoCommitIntervalMs))){
				System.out.println("Setting auto offset");}

			if (!(("NULL").equalsIgnoreCase(autoOffsetReset))){
			properties.put("auto.offset.reset", autoOffsetReset);
			}
				

			ConsumerConfig consumerConfig = new ConsumerConfig(properties);
			consumerConnector = Consumer
					.createJavaConsumerConnector(consumerConfig);
		} catch (NumberFormatException e) {
			System.out.println("Please check all consumer properties passed");
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			System.out.println("Please check all consumer properties passed");
			e.printStackTrace();
		} catch (org.I0Itec.zkclient.exception.ZkTimeoutException e) {
			System.out.println("Unable to connect to zookeeper server");
			e.printStackTrace();
		} catch (InvalidConfigException e) {
			System.out
					.println("Invalid value set for consumer properties. Please check all consumer properties passed");
			e.printStackTrace();
		}
	}

	/**
	 * Reads messages from the topic passed as parameter.Waits for a time
	 * specified by consumer timeout property and then returns the messages
	 * 
	 * @param topicName
	 *            :The topic from which message is to be read
	 * @return success: If consumer was started successfully
	 */

	public boolean startConsumer(String topicName) {

		boolean success = true;

		try {
			Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
			topicCountMap.put(topicName, new Integer(1));
			Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector
					.createMessageStreams(topicCountMap);

			KafkaStream<byte[], byte[]> stream = consumerMap.get(topicName)
					.get(0);

			it = stream.iterator();
			messagesList = new ArrayList<String>();
		} 
		catch (ConsumerTimeoutException ex)
		{			
			System.out.println("No new messages pushed within timeout threshold");
			success=false;
		} 
		catch (UnknownTopicOrPartitionException e) {

			System.out.println("Unknown topic or partition");
			success=false;
			e.printStackTrace();
			
		}
		catch(IllegalArgumentException e){
			System.out.println("Please check all parameters passed");
			success=false;
			e.printStackTrace();
		}
		catch(Exception e){
			success=false;
			e.printStackTrace();
		}
		finally{
		return success;
		}
	}

	/**
	 * For Getting The Next Available Message
	 * 
	 * @return nextMessage:String
	 */

	public String tail() {

		String message ="";

		try {
			if (it.hasNext())
				message = new String(it.next().message());
		}
		catch(NullPointerException ex){
			System.out.println("Message empty");
		}
		catch (ConsumerTimeoutException ex)
		{			
			System.out.println("No new messages pushed within timeout threshold");
		} 
		catch (UnknownTopicOrPartitionException e) {

			System.out.println("Unknown topic or partition");
			e.printStackTrace();
			
		}
		catch(IllegalArgumentException e){
			System.out.println("Please check all parameters passed");
			e.printStackTrace();
		}
		catch(Exception e){
			e.printStackTrace();
		}
		finally{
		return message;
		}
	}
	
	/**
	 * For getting all the messages available
	 * @return List of Strings
	 */
	public String[] poll() {

		messagesList.clear();
		
		try {
			while (it.hasNext()){
				messagesList.add(new String(it.next().message()));
			}

		}
		catch(NullPointerException ex){
			System.out.println("Message empty");
		}
		catch (ConsumerTimeoutException ex)
		{			
			System.out.println("No new messages pushed within timeout threshold");
		} 
		catch (UnknownTopicOrPartitionException e) {

			System.out.println("Unknown topic or partition");
			e.printStackTrace();
			
		}
		catch(IllegalArgumentException e){
			System.out.println("Please check all parameters passed");
			e.printStackTrace();
		}
		catch(Exception e){
			e.printStackTrace();
		}
		finally{
			String msgs[]=new String[messagesList.size()];
			msgs=messagesList.toArray(msgs);
		return msgs;
		}
	}
	

	/**
	 * Closes the consumer connector
	 */
	public void close() {
		consumerConnector.shutdown();
	}
}
