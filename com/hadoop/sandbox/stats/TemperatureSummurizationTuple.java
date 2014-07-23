package com.hadoop.sandbox.stats;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class TemperatureSummurizationTuple implements Writable{

	double mean;
	double median;
	double standardDeviation;
	int count;
	int minTemperature;
	int maxTemperature;
	int airTemperature;
	
	
	public int getMinTemperature() {
		return minTemperature;
	}
	public void setMinTemperature(int minTemperature) {
		this.minTemperature = minTemperature;
	}
	public int getMaxTemperature() {
		return maxTemperature;
	}
	public void setMaxTemperature(int maxTemperature) {
		this.maxTemperature = maxTemperature;
	}
	public int getAirTemperature() {
		return airTemperature;
	}
	public void setAirTemperature(int airTemperature) {
		this.airTemperature = airTemperature;
	}
	public double getMean() {
		return mean;
	}
	public void setMean(double mean) {
		this.mean = mean;
	}
	public double getMedian() {
		return median;
	}
	public void setMedian(double median) {
		this.median = median;
	}
	public double getStandardDeviation() {
		return standardDeviation;
	}
	public void setStandardDeviation(double standardDeviation) {
		this.standardDeviation = standardDeviation;
	}
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	
	@Override
	public void readFields(DataInput input) throws IOException {
		// TODO Auto-generated method stub
		count=input.readInt();
		mean=input.readDouble();
		median=input.readDouble();
		standardDeviation=input.readDouble();
		minTemperature=input.readInt();
		maxTemperature=input.readInt();
		airTemperature=input.readInt();
	}
	
	
	@Override
	public void write(DataOutput output) throws IOException {
		// TODO Auto-generated method stub
		output.writeInt(getCount());
		output.writeDouble(getMean());
		output.writeDouble(getMedian());
		output.writeDouble(getStandardDeviation());
		output.writeInt(getMinTemperature());
		output.writeInt(getMaxTemperature());
		output.writeInt(getAirTemperature());
	}
	
	
	@Override
	public String toString(){
		return String.valueOf(minTemperature)+"  "+String.valueOf(maxTemperature)+" "+String.valueOf(mean)+"  "+String.valueOf(median)+"  "+String.valueOf(standardDeviation)+"  ";
	}
	
	
}
