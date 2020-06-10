package com.gusi.flink.simple.tabel;

import com.gusi.flink.simple.StreamDTO;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * TableApiDemo <br>
 *
 * @author Lucky
 * @since 2020/5/22
 */
public class TableApiDemo {
	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		DataStreamSource<String> sourceString = env.socketTextStream("localhost", 9999);
		DataStream<StreamDTO> sourceStream = sourceString.map(line -> {
			return new StreamDTO("1", 1, 1L);
		});


		TableEnvironment tableEnv = StreamTableEnvironment.create(env);

		// Table view1 = tableEnv.from("");
		//  tableEnv.createTemporaryView("",view1);
		//  view1.
		// table.window(Tumble over 10000. millis on 'ts as ' tt).groupBy('ch,'tt).select('ch, 'ch.count)


		try {
			tableEnv.execute("table job");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
