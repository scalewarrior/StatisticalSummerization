package com.hadoop.sandbox.stats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class TemperatureSummerizationReducer   extends Reducer<Text, TemperatureSummurizationTuple,Text,Text> {

	@Override
	  public void reduce(Text key, Iterable<TemperatureSummurizationTuple> values,
		      Context context)
		      throws IOException, InterruptedException {
		    
		    List<Integer> temperatureList=new ArrayList<Integer>();
		    int temperature=0;
		    int sum=0;
		    int count=0;
		    
		  for (TemperatureSummurizationTuple value : values) {
			  temperature=value.getAirTemperature();
			  temperatureList.add(temperature);
			  sum+=temperature;
			  count++;
		    }
		    
		 Collections.sort(temperatureList);
		 
		 double mean=sum/count; 
		 double standardDeviation=getStandardDeviation( mean,count,temperatureList);
		 double  median=getMedian(count,temperatureList);
		
		 int maxTemperature = getMaxtemp(temperatureList);
		 int minTemperature = getMintemp(temperatureList);
		 
		  TemperatureSummurizationTuple outTemperatureSummurizationTuple=new TemperatureSummurizationTuple();
		  outTemperatureSummurizationTuple.setMinTemperature(minTemperature);
		  outTemperatureSummurizationTuple.setMaxTemperature(maxTemperature);
		  outTemperatureSummurizationTuple.setMean(mean);
		  outTemperatureSummurizationTuple.setMedian(median);
		  outTemperatureSummurizationTuple.setStandardDeviation(standardDeviation);
		  outTemperatureSummurizationTuple.setCount(count);
		  
		  context.write(key, new Text(outTemperatureSummurizationTuple.toString() +String.valueOf( count)));

	  }
	  
	  
    @SuppressWarnings("unused")
	private double getStandardDeviation(double mean,int count,List<Integer> temperatureList){
		  double sumOfSquares=0.0;
		  double standardDeviation=0.0;
		  
			 for(int airTemperature:temperatureList){
					sumOfSquares += ((airTemperature -mean) * (airTemperature -mean));
					standardDeviation= (double) Math.sqrt(sumOfSquares / (count-1));
		}
			 
			return standardDeviation;  
	  }
    
    @SuppressWarnings("unused")
	private double getMedian(int count,List<Integer> temperatureList){
    	
    double median=0.0;
    	
    if((count % 2) == 0){
		   median= ((temperatureList.get(count/2) - 1) +  temperatureList.get(count/2) )/2;
	   }
	   else{
		   median=temperatureList.get(count/2);
	   }
	return median;
    }
    
    private int getMaxtemp(List<Integer> temperatureList){
    	  
	    int maxValue = Integer.MIN_VALUE;
	    for (Integer value : temperatureList) {
	      maxValue = Math.max(maxValue, value);
	    }
		return maxValue;
    }
    
    private int getMintemp(List<Integer> temperatureList){
    	 int maxValue = Integer.MAX_VALUE;
 	    for (Integer value : temperatureList) {
 	           maxValue = Math.min(maxValue, value);
 	        }
 		return maxValue;
    }
}
