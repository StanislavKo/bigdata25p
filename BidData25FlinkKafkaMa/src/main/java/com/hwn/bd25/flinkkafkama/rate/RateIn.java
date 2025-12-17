package com.hwn.bd25.flinkkafkama.rate;

public class RateIn {

	private String symbol;
	private Long ts;
	private Double price;
	private Double volume;

	public RateIn() {
		super();
		// TODO Auto-generated constructor stub
	}

	public RateIn(String symbol, Long ts, Double price, Double volume) {
		super();
		this.symbol = symbol;
		this.ts = ts;
		this.price = price;
		this.volume = volume;
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

	public Double getVolume() {
		return volume;
	}

	public void setVolume(Double volume) {
		this.volume = volume;
	}

}
