package com.autentia.tutoriales;

public enum MeasureType {

	CO("co", 1),
	NO("no", 2),
	NO2("no2", 3),
	O3("o3", 4),
	PM10("pm10", 5),
	SH2("sh2", 6),
	PM25("pm25", 7),
	PST("pst", 8),
	SO2("so2", 9);

	private final String type;
	private final int order;

	private MeasureType(String value, int order) {
		this.type = value;
		this.order = order;
	}

	public String getType() {
		return type;
	}

	public static int getOrder(String type) {

		for (MeasureType measureType : MeasureType.values()) {
			if (measureType.getType().equals(type)) {
				return measureType.order;
			}
		}

		// Value by default
		return MeasureType.CO.order;
	}
}
