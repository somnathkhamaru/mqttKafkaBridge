package com.bits.iot.kafkaStreamProcessor.model;

import java.io.Serializable;
import java.sql.Timestamp;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DriverData implements Serializable {

	private static final long serialVersionUID = 1L;
	private String driver_id;
	private String route_id;
	private int speed;
	private String truck_id;
	private String event_timestamp;

	public DriverData() {

	}

	@JsonCreator
	public DriverData(@JsonProperty("driver_id") String driver_id, @JsonProperty("route_id") String route_id,
			@JsonProperty("speed") int speed, @JsonProperty("truck_id") String truck_id ,@JsonProperty("event_timestamp") String event_timestamp) {
		this.setDriver_id(driver_id);
		this.setRoute_id(route_id);
		this.setSpeed(speed);
		this.setTruck_id(truck_id);
		this.setEvent_timestamp(event_timestamp);
		
	}

	public String getDriver_id() {
		return driver_id;
	}

	public void setDriver_id(String driver_id) {
		this.driver_id = driver_id;
	}

	public int getSpeed() {
		return speed;
	}

	public void setSpeed(int speed) {
		this.speed = speed;
	}

	public String getRoute_id() {
		return route_id;
	}

	public void setRoute_id(String route_id) {
		this.route_id = route_id;
	}

	public String getTruck_id() {
		return truck_id;
	}

	public void setTruck_id(String truck_id) {
		this.truck_id = truck_id;
	}

	public String getEvent_timestamp() {
		return event_timestamp;
	}

	public void setEvent_timestamp(String event_timestamp) {
		this.event_timestamp = event_timestamp;
	}

	
}
