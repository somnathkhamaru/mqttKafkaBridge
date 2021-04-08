package com.bits.iot.kafkaStreamProcessor;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology.AutoOffsetReset;

import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import com.bits.iot.kafkaStreamProcessor.model.DriverData;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class StreamProcessor {

	static HashMap<String, HashMap<String, Integer>> driverDataMapAgg = new HashMap<String, HashMap<String, Integer>>();
	static HashMap<String, HashMap<String, Integer>> truckDataMapAgg = new HashMap<String, HashMap<String, Integer>>();
	static HashMap<String, HashMap<String, Integer>> routeDataMapAgg = new HashMap<String, HashMap<String, Integer>>();

	public static void main(String[] args) {

		Properties streamsConfiguration = new Properties();
		streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, IKafkaConstants.APP_ID);
		streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConstants.KAFKA_BROKERS);
		StreamsBuilder builder = new StreamsBuilder();

		JsonSerializer<DriverData> driverDataJsonSerializer = new JsonSerializer<>();
		JsonDeserializer<DriverData> driverDataJsonDeserializer = new JsonDeserializer<>(DriverData.class);
		Serde<DriverData> driverDataSerde = Serdes.serdeFrom(driverDataJsonSerializer, driverDataJsonDeserializer);

		try {

			KStream<String, DriverData> kStream = builder.stream(IKafkaConstants.KAFKA_TOPIC_NAME,
					org.apache.kafka.streams.Consumed.with(Serdes.String(), driverDataSerde)
							.withOffsetResetPolicy(AutoOffsetReset.LATEST));
			System.out.println("Stream Processing Started");
			kStream.foreach(new ForeachAction<String, DriverData>() {

				public void apply(String arg0, DriverData arg1) {

					if (arg1.getSpeed() > IKafkaConstants.OVER_SPEED_LIMIT) {
						System.out.println("Speed Limit Crossed");
						enrichData(arg1);
					}

				}

			});
		} catch (Exception s) {
			s.printStackTrace();
		}
		KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration);
		kafkaStreams.start();

	}

	public static void enrichData(DriverData arg1) {
		String driverID = arg1.getDriver_id();
		String truckID = arg1.getTruck_id();
		String routeID = arg1.getRoute_id();
		String year_month = arg1.getEvent_timestamp().substring(0, 7);
		System.out.println("YearMonth:" + year_month);

		// driverMetrics
		if (driverDataMapAgg.containsKey(driverID)) {
			HashMap<String, Integer> driverDataMapins = driverDataMapAgg.get(driverID);
			if (driverDataMapins.containsKey(year_month)) {
				driverDataMapins.put(year_month, driverDataMapins.get(year_month) + 1);
			} else {
				driverDataMapins.put(year_month, 1);
			}

			driverDataMapAgg.put(driverID, driverDataMapins);
		} else {
			HashMap<String, Integer> driverDataMap = new HashMap<String, Integer>();
			driverDataMap.put(year_month, 1);
			driverDataMapAgg.put(driverID, driverDataMap);
		}

		// RouteMetrics
		if (routeDataMapAgg.containsKey(routeID)) {
			HashMap<String, Integer> routeDataMapins = routeDataMapAgg.get(routeID);
			if (routeDataMapins.containsKey(year_month)) {
				routeDataMapins.put(year_month, routeDataMapins.get(year_month) + 1);
			} else {
				routeDataMapins.put(year_month, 1);
			}

			routeDataMapAgg.put(routeID, routeDataMapins);
		} else {
			HashMap<String, Integer> routeDataMap = new HashMap<String, Integer>();
			routeDataMap.put(year_month, 1);
			routeDataMapAgg.put(routeID, routeDataMap);
		}

		// TruckMetrics
		if (truckDataMapAgg.containsKey(truckID)) {
			HashMap<String, Integer> truckDataMapins = truckDataMapAgg.get(truckID);
			if (truckDataMapins.containsKey(year_month)) {
				truckDataMapins.put(year_month, truckDataMapins.get(year_month) + 1);
			} else {
				truckDataMapins.put(year_month, 1);
			}

			truckDataMapAgg.put(routeID, truckDataMapins);
		} else {
			HashMap<String, Integer> truckDataMap = new HashMap<String, Integer>();
			truckDataMap.put(year_month, 1);
			truckDataMapAgg.put(truckID, truckDataMap);
		}

	
		String driverSpjson = "";
		String truckSpjson = "";
		String routeSpjson = "";
		String spjson = "";
		try {
			driverSpjson = new ObjectMapper().writeValueAsString(driverDataMapAgg);
			truckSpjson = new ObjectMapper().writeValueAsString(truckDataMapAgg);
			routeSpjson = new ObjectMapper().writeValueAsString(routeDataMapAgg);
			spjson = new ObjectMapper().writeValueAsString(arg1);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Driver Over speed json:" + driverSpjson);
		System.out.println("Truck Over speed json:" + truckSpjson);
		System.out.println("Route Over speed json:" + routeSpjson);
		System.out.println("OverSpeeder:" + spjson);
		runProducer(truckSpjson, IKafkaConstants.TRUCK_OVER_SPEEDER_METRICS);
		runProducer(routeSpjson, IKafkaConstants.ROUTE_OVER_SPEEDER_METRICS);
		runProducer(driverSpjson, IKafkaConstants.DRIVER_OVER_SPEEDER_METRICS);
		runProducer(spjson,IKafkaConstants.OVER_SPEEDER);

	}

	public static void runProducer(String Payload, String topic) {
		Producer<Long, String> producer = ProducerCreator.createProducer();

		final ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(topic, Payload);
		try {
			RecordMetadata metadata = producer.send(record).get();
			System.out
					.println("Record sent to partition " + metadata.partition() + " with offset " + metadata.offset());
		} catch (ExecutionException e) {
			System.out.println("Error in sending record");
			System.out.println(e);
		} catch (InterruptedException e) {
			System.out.println("Error in sending record");
			System.out.println(e);
		}

	}
}
