package org.redoop.audits;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AuditLogsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private static final String ADD_GROUP = "ADD_GROUP";// TODO
	private static final String ADD_USER = "ADD_USER";// TODO
	
	private static final String CRED_ACQ = "CRED_ACQ"; // DONE
	private static final String CRED_DISP = "CRED_DISP"; // DONE
	private static final String CRED_REFR = "CRED_REFR"; //TODO
	
	private static final String CRYPTO_KEY_USER = "CRYPTO_KEY_USER"; //TODO
	private static final String CRYPTO_SESSION = "CRYPTO_SESSION"; // DONE
	
	private static final String DEL_GROUP = "DEL_GROUP";// TODO
	private static final String DEL_USER = "DEL_USER";// TODO
	
	private static final String LOGIN = "LOGIN"; // DONE
	
	private static final String USER_ACCT = "USER_ACCT"; // DONE
	private static final String USER_AUTH = "USER_AUTH"; // DONE
	private static final String USER_END = "USER_END"; // DONE
	private static final String USER_LOGIN = "USER_LOGIN"; // DONE
	private static final String USER_LOGOUT = "USER_LOGOUT"; // DONE
	private static final String USER_START = "USER_START"; // DONE
	
	public void map(LongWritable ikey, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		
		HashMap<String, Object> map = new HashMap<String, Object>();
		String fields[] = line.split(" ");
		String type = fields[1].split("=")[1];
		
		
		switch(type){
		
			case(USER_LOGIN):
				
				if(fields[14].split("=")[1].contains("success")){
					context.write(new Text(fields[15].split("=")[1]), new IntWritable(1));
				}
			
//				map.put("node", fields[0].split("=")[1]);
//				map.put("type", type);
//				map.put("msgaudit", fields[2].split("=")[1]);
//				map.put("aux", fields[3]);
//				map.put("pid", fields[4].split("=")[1]);
//				map.put("uid", fields[5].split("=")[1]);
//				map.put("auid", fields[6].split("=")[1]);
//				map.put("ses", fields[7].split("=")[1]);
//				map.put("op", fields[8].split("=")[2]);
//				map.put("acct", fields[9].split("=")[1]);
//				map.put("exe", fields[10].split("=")[1]);
//				map.put("hostname", fields[11].split("=")[1]);
//				map.put("addr", fields[12].split("=")[1]);
//				map.put("terminal", fields[13].split("=")[1]);
//				map.put("res", fields[14].split("=")[1]);
				break;
			
		}
		
	}

}
