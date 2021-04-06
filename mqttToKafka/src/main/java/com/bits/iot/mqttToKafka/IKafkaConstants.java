package com.bits.iot.mqttToKafka;

public class IKafkaConstants {
	public static String KAFKA_BROKERS = "localhost:9092";

	public static Integer MESSAGE_COUNT = 1000;

	public static String CLIENT_ID = "client1";

	public static String TOPIC_NAME = "demo";

	public static String GROUP_ID_CONFIG = "consumerGroup10";

	public static Integer MAX_NO_MESSAGE_FOUND_COUNT = 100;

	public static String OFFSET_RESET_LATEST = "latest";

	public static String OFFSET_RESET_EARLIER = "earliest";

	public static Integer MAX_POLL_RECORDS = 1;
	
	public static String MQTT_TOPIC = "despo";
	
	public static String MQTT_BROKER = "tcp://broker.hivemq.com:1883";

}
