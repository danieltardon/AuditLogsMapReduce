package org.redoop.audits;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InvalidInputException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AuditLogs {

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: AuditLogs <input path> <output path>");
			System.exit(-1);
		}
		
		//SET JOB CONFIG AND CLASSES
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Audit Logs Eclipse (Logins)");
		job.setJarByClass(org.redoop.audits.AuditLogs.class);
		job.setMapperClass(org.redoop.audits.AuditLogsMapper.class);
		job.setReducerClass(org.redoop.audits.AuditLogsReducer.class);

		// Specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// DELETE OUTPUT DIRECTORY FOR DEVELOPMENT
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(args[1]), true);
		
		// SET INPUT PATHS
		List<IOException> rslt = new ArrayList<IOException>();
		//String p = "/user/camus/data/audits/monthly/2014/*";
		String p = args[0];
		FileStatus[] inputs = fs.globStatus(new Path(p));
		if(inputs.length > 0) {
		      for (FileStatus onePath: inputs) {
		    	  FileInputFormat.addInputPath(job, onePath.getPath());
		      }
		} else {
		      rslt.add(new IOException("Input source " + p + " does not exist."));
		}
		if (!rslt.isEmpty()) {
		    throw new InvalidInputException(rslt);
		}

		// SET OUTPUT PATH
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
