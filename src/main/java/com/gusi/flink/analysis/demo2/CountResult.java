package com.gusi.flink.analysis.demo2;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * UvCount <br>
 *
 * @author Lucky
 * @since 2020/5/12
 */
public class CountResult {
	private long endTime;
	private String type;
	private long count;

	public CountResult() {
	}

	public CountResult(long endTime, String type, long count) {
		this.endTime = endTime;
		this.type = type;
		this.count = count;
	}

	public long getEndTime() {
		return endTime;
	}

	public void setEndTime(long endTime) {
		this.endTime = endTime;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	@Override
	public String toString() {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		return "UvCount{" +
				"endTime=" + format.format(new Date(endTime)) +
				", type='" + type + '\'' +
				", count=" + count +
				'}';
	}
}
