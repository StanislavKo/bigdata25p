package com.hwn.bd25.kafkaconnectwssource.rate;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RateIn {

	private String s;
	private Long t;
	private Double p;
	private Double v;

	public RateIn() {
		super();
		// TODO Auto-generated constructor stub
	}

	public RateIn(String s, Long t, Double p, Double v) {
		super();
		this.s = s;
		this.t = t;
		this.p = p;
		this.v = v;
	}

	public String getS() {
		return s;
	}

	public void setS(String s) {
		this.s = s;
	}

	public Long getT() {
		return t;
	}

	public void setT(Long t) {
		this.t = t;
	}

	public Double getP() {
		return p;
	}

	public void setP(Double p) {
		this.p = p;
	}

	public Double getV() {
		return v;
	}

	public void setV(Double v) {
		this.v = v;
	}

}
