package com.hwn.bd25.sparkiceberg.rate;

public class MaOut {

	private String time;
	private String symbol;
	private Long ts;
	private Double price;

	public MaOut() {
		super();
		// TODO Auto-generated constructor stub
	}

	public MaOut(String time, String symbol, Long ts, Double price) {
		super();
		this.time = time;
		this.symbol = symbol;
		this.ts = ts;
		this.price = price;
	}

	public String getTime() {
		return time;
	}

	public void setTime(String time) {
		this.time = time;
	}

	public String getSymbol() {
		return symbol;
	}

	public void setSymbol(String symbol) {
		this.symbol = symbol;
	}

	public Long getTs() {
		return ts;
	}

	public void setTs(Long ts) {
		this.ts = ts;
	}

	public Double getPrice() {
		return price;
	}

	public void setPrice(Double price) {
		this.price = price;
	}

}
