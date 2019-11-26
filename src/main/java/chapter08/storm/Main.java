package chapter08.storm;

import chapter08.storm.bolt.DataBaseLoadBolt;
import chapter08.storm.bolt.MapSearchBolt;
import chapter08.storm.bolt.SpeedProcessBolt;
import chapter08.storm.spout.SocketSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class Main {

	public static void main(String[] args) throws AlreadyAliveException,
			InvalidTopologyException, InterruptedException {
		// FieldListenerSpout fieldListenerSpout = new FieldListenerSpout();
		SocketSpout socketSpout = new SocketSpout();
		MapSearchBolt mapSearchBolt = new MapSearchBolt();
		// SpeedCalculatorBolt speedBolt =new SpeedCalculatorBolt();
		SpeedProcessBolt speedBolt = new SpeedProcessBolt();
		DataBaseLoadBolt dbLoadBlot = new DataBaseLoadBolt();
		TopologyBuilder builder = new TopologyBuilder();

		// builder.setSpout("spout", fieldListenerSpout,1);
		builder.setSpout("spout", socketSpout, 1);
		builder.setBolt("mapSearchBolt", mapSearchBolt, 4).shuffleGrouping(
				"spout");
		builder.setBolt("speedBolt", speedBolt, 1).fieldsGrouping(
				"mapSearchBolt", new Fields("roadID"));
		builder.setBolt("dbLoadBlot", dbLoadBlot, 1).shuffleGrouping(
				"speedBolt");

		Config conf = new Config();
		if (args != null && args.length > 0) {
			conf.setNumWorkers(60);
			StormSubmitter.submitTopology(args[0], conf,
					builder.createTopology());
		} else {
			conf.setDebug(true);
			conf.setMaxTaskParallelism(60);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("Threshold_Test", conf,
					builder.createTopology());
			Thread.sleep(3000);
			cluster.shutdown();
		}

	}

}
