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
		// StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// DataStreamSource<String> lines = env.socketTextStream("localhost", 9999);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		DataStreamSource<String> lines = env.socketTextStream("localhost", 9999);
		lines.print("line").setParallelism(1);

		SingleOutputStreamOperator<StreamDTO> sourceStream = lines.map((s) -> {
			String[] line = s.split(" ");
			return new StreamDTO(line[0], Double.parseDouble(line[1]), Long.parseLong(line[2]));
		});


		// Tuple的类型数据的map，需要用匿名内部类，不能用lamda表达式，因为lamda会导致泛型推断错误
		SingleOutputStreamOperator<Tuple3<String, Double, Long>> tupleStream = sourceStream.map(
				new MapFunction<StreamDTO, Tuple3<String, Double, Long>>() {
					@Override
					public Tuple3<String, Double, Long> map(StreamDTO s) throws Exception {
						Tuple3<String, Double, Long> t = new Tuple3<String, Double, Long>();
						t.f0 = s.getDtoId();
						t.f1 = s.getValue();
						t.f2 = s.getTimestamp();
						return t;
					}
				});

		tupleStream.print("tuple line").setParallelism(1);
		// sink to csv file。必须是tuple类型的数据
		DataStreamSink<Tuple3<String, Double, Long>> sinkCsvStream = tupleStream.setParallelism(1).writeAsCsv("test-output/test1.csv", FileSystem.WriteMode.OVERWRITE);
		// sink to txt file。
		DataStreamSink<StreamDTO> sinkTxtStream = sourceStream.writeAsText("test-output/test2.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

		try {
			env.execute("write to csv file job");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
