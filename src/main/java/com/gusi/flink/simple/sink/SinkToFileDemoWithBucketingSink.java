package com.gusi.flink.simple.sink;

import com.gusi.flink.simple.StreamDTO;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.io.RowCsvInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;

import java.io.File;
import java.time.ZoneId;
import java.util.concurrent.TimeUnit;

/**
 * SinkForFileDemo <br>
 *     BucketingSink 已过期，不建议使用
 *
 * @author Lucky
 * @since 2020/5/20
 */
public class SinkToFileDemoWithBucketingSink {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStreamSource<String> lines = env.socketTextStream("localhost", 9999);

		// 设置checkpoint
		env.enableCheckpointing(TimeUnit.SECONDS.toMillis(10));


		BucketingSink<String> sink = new BucketingSink<>("test1");
		//指定在/data/test/下的路径格式 '/data/test/2020-01-01/00'
		sink.setBucketer(new DateTimeBucketer<>("yyyy-MM-dd/HH", ZoneId.of("Asia/Shanghai")));
		//设置滚动大小 bytes，默认1024L * 1024L * 384L
		sink.setBatchSize(1024L * 1024L * 384L);
		// 设置滚动间隔 ms, 默认Long.MAX_VALUE
		sink.setBatchRolloverInterval(60000);
		//还有空闲检测等参数，可以自行配置


		lines.addSink(sink).setParallelism(1);

		env.execute("sink file job");
	}
}
