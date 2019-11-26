package chapter0203.bolt;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

@SuppressWarnings("serial")
public class WordCount implements IBasicBolt {
	private Map<String, Integer> _counts = new HashMap<String, Integer>();

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext context) {
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String word = tuple.getString(0);
		int count;
		if (_counts.containsKey(word)) {
			count = _counts.get(word);
		} else {
			count = 0;
		}
		count++;
		_counts.put(word, count);
		collector.emit(new Values(word, count));
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "count"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
}
