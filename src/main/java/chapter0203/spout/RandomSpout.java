package chapter0203.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

/**
 * @author csm spout：随机生成小于100的整数
 */
public class RandomSpout extends BaseRichSpout {

	private static final long serialVersionUID = -1463609235536028987L;
	private SpoutOutputCollector collector;
	private Random random;

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
		random = new Random();

	}

	@Override
	public void nextTuple() {
		while (true) {
			Values val = new Values(random.nextInt(100));
			collector.emit(val);
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("randomInt"));
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		super.close();
	}
}
