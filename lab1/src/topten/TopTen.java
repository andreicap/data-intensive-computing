package topten;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;

public class TopTen {
    
    // This helper function parses the stackoverflow into a Map for us.
    public static Map<String, String> transformXmlToMap(String xml) {
		
		Map<String, String> map = new HashMap<String, String>();
		try {
		    String[] tokens = xml.trim().substring(5, xml.trim().length() - 3).split("\"");
		    for (int i = 0; i < tokens.length - 1; i += 2) {
			String key = tokens[i].trim();
			String val = tokens[i + 1];
			map.put(key.substring(0, key.length() - 1), val);
		    }
		} catch (StringIndexOutOfBoundsException e) {
		    System.err.println(xml);
		}

		return map;

    }

    public static class TopTenMapper extends Mapper<Object, Text, NullWritable, Text> {
		
		// Stores a map of user reputation to the record
		TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		    
		    // Take a single XML node and parse it in a Map object
		    Map<String, String> parsedXml = transformXmlToMap(value.toString());
		    // Get the reputation of the node
			String reputation = parsedXml.get("Reputation");
			// The try-catch statement is used to handle excpetion due to missing/null values of Reputation
			try {
				// Insert the record in the map, the map stores record in ascendent order on Reputation
				repToRecordMap.put(Integer.parseInt(reputation), new Text(value));
				// If there are more than 10 records in the map, remove the record on top.
				// Since the map is ordered, the record on top will be the one with the lowest reputation
				if (repToRecordMap.size() > 10)
					repToRecordMap.remove(repToRecordMap.firstKey());
			} catch(Exception e) {
				System.out.println("Mapper - Reputation not parsable.");
			}
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			
			// Write the records in the context output to be handled by the Reducer
			for (Text t : repToRecordMap.values())
				context.write(NullWritable.get(), t);

	    }

	}

    public static class TopTenReducer extends TableReducer<NullWritable, Text, NullWritable> {
		
		// Stores a map of user reputation to the record
		private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

		public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		    
		    // For each node from the mapper, do the same process of the mapper,
		    // reducing the values to the ten with the highest reputation
		    for (Text value : values) {
				Map<String, String> parsed = transformXmlToMap(value.toString());
				try {
					repToRecordMap.put(Integer.parseInt(parsed.get("Reputation")), new Text(value));
				}
				catch(Exception e) {
					System.out.println("Reducer - Reputation not parsable.");
				}
				if (repToRecordMap.size() > 10)
					repToRecordMap.remove(repToRecordMap.firstKey());
			}

			// Use an Index to set the row number
			Integer index = 1;
			// FOr each record, get Id and Reputation and store it in a column of the topten table on Hbase
			for (Text t : repToRecordMap.descendingMap().values()) {
				// Create the HBase record
				Map<String, String> parsed = transformXmlToMap(t.toString());
				// Create the Put object to write it in context output
				Put insHBase = new Put(Bytes.toBytes(index));
				insHBase.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes(parsed.get("Id")));
				insHBase.addColumn(Bytes.toBytes("info"), Bytes.toBytes("rep"), Bytes.toBytes(parsed.get("Reputation")));
				context.write(null, insHBase);
				index++;
				System.out.println(parsed.get("Reputation"));
				System.out.println(parsed.get("Id"));
			}

		}
    }

    public static void main(String[] args) throws Exception {
		
		// General configuration
		Configuration conf = HBaseConfiguration.create();
		Job job = Job.getInstance(conf);
		job.setJarByClass(TopTen.class);
		// Set the mapper class
		job.setMapperClass(TopTenMapper.class);
		// Set the path to get the input file
		FileInputFormat.addInputPath(job, new Path(args[0]));

		// Set the output classes of the mapper
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		// Set the number of Reducer instances
		job.setNumReduceTasks(1);

		// Define the HBaase table in which writing the result
		TableMapReduceUtil.initTableReducerJob("topten", TopTenReducer.class, job);
		job.waitForCompletion(true);

    }

}
