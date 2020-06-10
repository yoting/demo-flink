package com.gusi.flink.simple.sink;

import com.gusi.flink.simple.StreamDTO;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * SinkToFileDemoWithStreamWriteFile <br>
 * 直接通过stream写入文件
 *
 * @author Lucky
 * @since 2020/5/21
 */
public class SinkToFileDemoWithStreamWriteFile {
	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStreamSource<String> lines = env.socketTextStream("localhost", 9999);
		lines.print("line");

		SingleOutputStreamOperator<StreamDTO> sourceStream = lines.map((s) -> {
			String[] line = s.split(" ");
			return new StreamDTO(line[0], Double.parseDouble(line[1]), Long.parseLong(line[2]));
		});

		sourceStream.addSink()


		// Tuple的类型数据的map
		// SingleOutputStreamOperator<Tuple3<String, Double, Long>> tupleStream = sourceStream.map(
		// 		new MapFunction<StreamDTO, Tuple3<String, Double, Long>>() {
		// 			@Override
		// 			public Tuple3<String, Double, Long> map(StreamDTO s) throws Exception {
		// 				Tuple3<String, Double, Long> t = new Tuple3<String, Double, Long>();
		// 				t.f0 = s.getDtoId();
		// 				t.f1 = s.getValue();
		// 				t.f2 = s.getTimestamp();
		// 				return t;
		// 			}
		// 		});
		SingleOutputStreamOperator<Tuple3<String, Double, Long>> tupleStream = sourceStream.map(
				(s) -> {
					Tuple3<String, Double, Long> t = new Tuple3<String, Double, Long>();
					t.f0 = s.getDtoId();
					t.f1 = s.getValue();
					t.f2 = s.getTimestamp();
					return t;
				});

		tupleStream.print("tuple").setParallelism(1);
		// sink to csv file。必须是tuple类型的数据
		DataStreamSink<Tuple3<String, Double, Long>> sinkCsvStream = tupleStream.writeAsCsv("test-output/test1.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
		// 写csv的实现是CsvOutputFormat来实现的，而CsvOutputFormat的内部用的是OutputStreamWriter,OutputStreamWriter内部是带buffer的BufferedOutputStream(this.stream, 4096)
		// 所以数据需要把buffer填充满了才会flush的文件，并不是以来就把数据刷到文件。

		// sink to txt file。
		DataStreamSink<StreamDTO> sinkTxtStream = sourceStream.writeAsText("test-output/test2.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);


		// TODO:官方文档提示：
		// DataStream的write和print的各种sink主要都是用来调试的，也没法保证数据exactly-once和故障恢复，正式场景应用.addSink(...);

		try {
			env.execute("write to csv file job");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
