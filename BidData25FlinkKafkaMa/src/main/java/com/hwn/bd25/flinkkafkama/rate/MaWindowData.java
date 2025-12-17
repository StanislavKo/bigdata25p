package com.hwn.bd25.flinkkafkama.rate;

import java.util.List;

public class MaWindowData {

	private List<RateIn> rates;

	public MaWindowData() {
		super();
		// TODO Auto-generated constructor stub
	}

	public MaWindowData(List<RateIn> rates) {
		super();
		this.rates = rates;
	}

	public List<RateIn> getRates() {
		return rates;
	}

	public void setRates(List<RateIn> rates) {
		this.rates = rates;
	}

}
