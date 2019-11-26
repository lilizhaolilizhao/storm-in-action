package chapter04.storm;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

@SuppressWarnings("serial")
public class ReaderSpout extends BaseRichSpout {
	SpoutOutputCollector _collector;

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;
	}

	@Override
	public void nextTuple() {

		String uri = "hdfs://master:9000/storm/m.txt";
		InputStream in = null;
		try {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(uri), conf);
			in = fs.open(new Path(uri));
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String line = null;
			while (null != (line = br.readLine())) {
				_collector.emit(new Values(line));
				Utils.sleep(100);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(in);
		}
	}

	@Override
	public void ack(Object id) {
	}

	@Override
	public void fail(Object id) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("ip"));
	}

}
