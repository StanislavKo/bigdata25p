package com.hwn.bd25.sparkclickhouse.rate;

public class Candle {

	private String symbol;
	private Long ts;
	private Integer period;
	private Double minPrice;
	private Double maxPrice;

	public Candle() {
		super();
		// TODO Auto-generated constructor stub
	}

	public Candle(String symbol, Long ts, Integer period, Double minPrice, Double maxPrice) {
		super();
		this.symbol = symbol;
		this.ts = ts;
		this.period = period;
		this.minPrice = minPrice;
		this.maxPrice = maxPrice;
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

	public Integer getPeriod() {
		return period;
	}

	public void setPeriod(Integer period) {
		this.period = period;
	}

	public Double getMinPrice() {
		return minPrice;
	}

	public void setMinPrice(Double minPrice) {
		this.minPrice = minPrice;
	}

	public Double getMaxPrice() {
		return maxPrice;
	}

	public void setMaxPrice(Double maxPrice) {
		this.maxPrice = maxPrice;
	}

}
