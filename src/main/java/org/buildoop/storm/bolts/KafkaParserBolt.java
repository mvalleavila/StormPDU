package org.buildoop.storm.bolts;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.util.Date;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;

import static backtype.storm.utils.Utils.tuple;

@SuppressWarnings("serial")
public class KafkaParserBolt implements IBasicBolt {
	
	private String index;
	//private String type;

	@SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context) {
    	index = (String) stormConf.get("elasticsearch.index");
    	//this.type = (String) stormConf.get("elasticsearch.type");
    }

    public void execute(Tuple input, BasicOutputCollector collector) {
    	String kafkaEvent = new String(input.getBinary(0));
    	
    	
    	if (kafkaEvent.length()>0)
    	{
    		String[] line = kafkaEvent.split(",");
        	String id = line[0];
        	System.out.println(kafkaEvent);
        	DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    		
    		String document;
    		
    		
    		document = "{\"hcan_SiteName\":\""+line[1]+"\",";
    		
    		document = document+"\"hcan_Power\":"+line[4]+",";
    		
    		try{
    		Date date = null;
    		date = dateFormat.parse(line[3]);
    		String time = String.valueOf(date.getTime());
    		document = document+"\"hcan_Fecha\":"+time+",";
    		
    		document = document+"\"hcan_RoomName\":\""+line[2]+"\",";
    		
    		document = document+"\"hcan_Temp\":"+line[5]+",";
    		
    		document = document+"\"hcan_Humidity\":"+line[6]+",";
    		
    		date = dateFormat.parse(line[7]);
    		time = String.valueOf(date.getTime());
    		document = document+"\"hcan_Timestamp\":"+time+"}";

    		
        	System.out.println("key:"+id);
        	System.out.println("index:"+index);
        	System.out.println("type:"+line[1]);
        	System.out.println("document:"+document);
    		} catch (ParseException e) {
    			e.printStackTrace();
    		}
        	
        	collector.emit(tuple(id,index, line[1], document));
    	}
    	
    	
    	
		
    }

	public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "index", "type", "document"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
    

}