package chapter0203.bolt;

import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

@SuppressWarnings("serial")
public class SplitSentence implements IBasicBolt {
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext context) {
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String sentence = tuple.getString(0);
		for (String word : sentence.split(" ")) {
			collector.emit(new Values(word));
		}
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
}
