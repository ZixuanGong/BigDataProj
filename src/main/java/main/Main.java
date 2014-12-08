package main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

public class Main {

public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		
		new plan_three.Clusterer(new Configuration());
//		new plan_one.Clusterer(new Configuration());
		
	}

}
