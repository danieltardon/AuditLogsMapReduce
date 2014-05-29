package org.redoop.audits.elasticsearch;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuditLogsESMapper extends Mapper <LongWritable, Text, LongWritable, MapWritable> {
	private static Logger log = LoggerFactory.getLogger(AuditLogsESMapper.class);
	
	@Override
	 protected void map(LongWritable key, Text value, Context context)
	        throws IOException, InterruptedException {
		
		try{
			log.info("Starting Map Job");
			String line = value.toString();
	        String fields[] = line.split(" ");
			String type = fields[1].split("=")[1];
			
			// create the MapWritable object
			MapWritable doc = new MapWritable();
			   
			//PARSE ONLY SUCCESSFUL USER_LOGIN EVENTS
			if (type.equalsIgnoreCase("USER_LOGIN") && line.contains("res=success")){
				
				String node = fields[0].split("=")[1];
				doc.put(new Text("node"), new Text(node));
				
				String from = fields[11].split("=")[1];
				doc.put(new Text("from"), new Text(from));
			
				String username = fields[15].split("=")[1];
				doc.put(new Text("username"), new Text(username));
				
				String ip = fields[12].split("=")[1];
				doc.put(new Text("ip"), new Text(ip));
				
				String timestamp = fields[2].substring(10).split(":")[0].substring(0, fields[0].length()-4);
				doc.put(new Text("timestamp"), new Text(timestamp));
				
				context.write(key, doc);
			}
		
		}catch (Exception e) {
	        e.printStackTrace();
	    }

	 }
}