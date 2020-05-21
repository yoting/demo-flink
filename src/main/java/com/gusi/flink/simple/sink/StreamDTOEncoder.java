package com.gusi.flink.simple.sink;

import com.gusi.flink.simple.StreamDTO;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.Encoder;

import java.io.IOException;
import java.io.OutputStream;

/**
 * StreamDTOEncoder <br>
 *
 * @author Lucky
 * @since 2020/5/21
 */
@PublicEvolving
public class StreamDTOEncoder implements Encoder<StreamDTO> {
	@Override
	public void encode(StreamDTO element, OutputStream stream) throws IOException {
		String line = element.getDtoId() + "," + element.getValue() + "," + element.getTimestamp();
		stream.write(line.toString().getBytes("UTF-8"));
		stream.write('\n');
	}
}
