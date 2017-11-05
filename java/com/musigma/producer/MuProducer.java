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

import kafka.common.FailedToSendMessageException;
import kafka.common.InvalidConfigException;
import kafka.common.UnknownTopicOrPartitionException;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerClosedException;
import kafka.producer.ProducerConfig;

/**
 * @author Shruti Gupta
 *
 */
public class MuProducer {

	/**
	 * Every instance of MuProducer is associated with a KAFKA producer
	 */
	private Producer<String, String> producer;

	/**
	 * @param props
	 *            The KAFKA producer is instantiated with the properties object
	 *            returned by setProducerProperties function of
	 *            ProducerProperties class
	 */
	public MuProducer(Properties props) {

		try {
			ProducerConfig config = new ProducerConfig(props);
			producer = new Producer<String, String>(config);
		} catch (NumberFormatException e) {
			System.out.println("Check all producer properties passed");
			e.printStackTrace();
		} catch (InvalidConfigException e) {
			System.out
			.println("Invalid value set for producer properties. Please check all producer properties passed");
			e.printStackTrace();
		}
		catch(IllegalArgumentException e){
			System.out.println("Please check all consumer properties passed");
			e.printStackTrace();
		}
		
		catch(Exception e)
		{
				e.printStackTrace();
		}

	}

	/**
	 * @param topicName
	 *            Name of the topic to which the producer pushes the msg
	 * @param ip
	 *            ip address where brokers are located
	 * @param msg
	 *            Message to be pushed to the topic
	 */
	public void sendMessage(String topicName, String ip, String msg) {

		try {
			KeyedMessage<String, String> data = new KeyedMessage<String, String>(
					topicName, ip, msg);
			producer.send(data);
		} catch (FailedToSendMessageException e) {

			System.out
			.println("Failed to send message after three tries. Check ip address passed and if zookeeper and kafka server are running");
			e.printStackTrace();
		} catch (UnknownTopicOrPartitionException e) {

			System.out.println("Unknown topic or partition");
			e.printStackTrace();
			
		}
		catch(IllegalArgumentException e){
			System.out.println("Please check all parameters passed");
			e.printStackTrace();
		}
		catch(ProducerClosedException e){
			System.out.println("Producer already closed");
			e.printStackTrace();
		}
		catch(Exception e){
			e.printStackTrace();
		}

	}

	/**
	 * terminates the producer
	 */
	public void close() {
		producer.close();

	}

}
