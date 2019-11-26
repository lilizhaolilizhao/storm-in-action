package chapter0203.spout;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
/**
  * @author csm
 *
 */
public class WordSplitBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5856179550908037438L;

	@SuppressWarnings({ "unused", "rawtypes" })
	private Map stormConf;
	@SuppressWarnings("unused")
	private TopologyContext context;
	private OutputCollector collector;
	
	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.stormConf = stormConf;
		this.context = context;
		this.collector = collector;
	}
	

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}
	
	public void execute(Tuple input) {
		String line = input.getString(0);
		String[] words = line.split(" ");
		for(int i=0;i<words.length;i++){
			String word = words[i].trim();
			if(!word.isEmpty()){
				word = word.toLowerCase();
				this.collector.emit(new Values(word));
			}
		}
	}
	
	public void cleanup() {
		super.cleanup();
	}


}
