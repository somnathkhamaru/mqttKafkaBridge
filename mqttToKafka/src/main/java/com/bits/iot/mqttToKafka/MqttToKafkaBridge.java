package com.bits.iot.mqttToKafka;


import java.util.concurrent.ExecutionException;


import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttSecurityException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class MqttToKafkaBridge {

	public static void main(String[] args) {
		try {
			final MqttClient client = new MqttClient(IKafkaConstants.MQTT_BROKER, // URI
					MqttClient.generateClientId(), // ClientId
					new MemoryPersistence());
			client.setCallback(new MqttCallback() {

				public void connectionLost(Throwable cause) {
					// Called when the client lost the connection to the broker
					System.out.println("dis-connected");
					try {
						client.connect();
					} catch (MqttSecurityException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (MqttException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					if (client.isConnected()) {
						System.out.println("re-connected");
					}
				}

				public void messageArrived(String topic, MqttMessage message) throws Exception {
					String payload = new String(message.getPayload());
					System.out.println(topic + ":" + payload);
					runProducer(payload);
				}

				public void deliveryComplete(IMqttDeliveryToken token) {// Called when a outgoing publish is complete
				}

			});

			client.connect();
			client.subscribe(IKafkaConstants.MQTT_TOPIC, 1);
			if (client.isConnected()) {
				System.out.println("connected");
			}
			while (client.isConnected()) {

			}
		} catch (MqttException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	static void runProducer(String Payload) {
		Producer<Long, String> producer = ProducerCreator.createProducer();

			final ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(IKafkaConstants.TOPIC_NAME,
					Payload);
			try {
				RecordMetadata metadata = producer.send(record).get();
				System.out.println("Record sent to partition " + metadata.partition()
						+ " with offset " + metadata.offset());
			} catch (ExecutionException e) {
				System.out.println("Error in sending record");
				System.out.println(e);
			} catch (InterruptedException e) {
				System.out.println("Error in sending record");
				System.out.println(e);
			}
		
	}
}
