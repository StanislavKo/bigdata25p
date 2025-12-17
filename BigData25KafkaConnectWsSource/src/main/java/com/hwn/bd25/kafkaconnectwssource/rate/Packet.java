package com.hwn.bd25.kafkaconnectwssource.rate;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Packet {

	private String type;
	private List<RateIn> data;

	public Packet() {
		super();
		// TODO Auto-generated constructor stub
	}

	public Packet(String type, List<RateIn> data) {
		super();
		this.type = type;
		this.data = data;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public List<RateIn> getData() {
		return data;
	}

	public void setData(List<RateIn> data) {
		this.data = data;
	}

}
