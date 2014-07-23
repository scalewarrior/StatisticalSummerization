package com.hadoop.sandbox.stats;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class TemperatureSummurizationMapper   extends Mapper<LongWritable, Text, Text, TemperatureSummurizationTuple> {
	private static final int MISSING = 9999;
	TemperatureSummurizationTuple temperatureSummurizationTuple=new TemperatureSummurizationTuple();
	
	  @Override
	  public void map(LongWritable key, Text value, Context context)
	      throws IOException, InterruptedException {
	    
	    String line = value.toString();
	    String year = line.substring(15, 19);
	    int airTemperature;
	    if (line.charAt(87) == '+') {
	    	// parseInt doesn't like leading plus signs
	      airTemperature = Integer.parseInt(line.substring(88, 92));
	    } else {
	      airTemperature = Integer.parseInt(line.substring(87, 92));
	    }
	    String quality = line.substring(92, 93);
	    if (airTemperature != MISSING && quality.matches("[01459]")) {
	    	temperatureSummurizationTuple.setAirTemperature(airTemperature/10);
	    	context.write(new Text(year), temperatureSummurizationTuple);
	    }      
	  }
}
