package com.openresearchinc.hadoop.sequencefile;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ucar.ma2.Array;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

//@formatter:off
/**
 * 
 * Detect music metadata/attribute from Million Song Database (MSD) (*.h5) encoded as Hadoop SequenceFile 
 * stored on hdfs:// or s3n://
 *  
 * HDF5 Dependencies: 
 * hdf5, hdf5-devel and jhdf5 packages
 * otherwise there will be error like: 
 * java.lang.UnsatisfiedLinkError: no hdf in java.library.path
 * 
 * P.S. This needs to be done for each node(slave), e.g., if used in AWS EMR.
 * @author heq
 */
// @formatter:on
public class HDF5 extends Configured implements Tool {
	final static Logger logger = LoggerFactory.getLogger(HDF5.class);
	final static String SEPARATOR = ":"; // separator between key and value
	static String attr;
	static String hostname = "localhost";

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new HDF5(), args);
		System.exit(res);
	}

	@Override
	public final int run(final String[] args) throws Exception {
		final String usage = "Usage: hadoop jar ./target/bin2seq*.jar com.openresearchinc.hadoop.sequencefile.HDF5  "
				+ "-libs $LIBJARS  <input-uri-h5-on-hdfs-or-s3> <output-uri-of-scanned-attributes-from-each file> "
				+ "-attr <id|tempo|artist|simiar_artist|..>";

		String[] otherArgs = new GenericOptionsParser(super.getConf(), args).getRemainingArgs();
		List<String> argList = Arrays.asList(otherArgs);

		if (argList.indexOf("-attr") != -1) {
			attr = otherArgs[argList.indexOf("-attr") + 1];
		} else {
			System.out.println(usage);
			System.exit(1);
		}
		Configuration conf = super.getConf();
		conf.set("attr", attr);
		Job job = Job.getInstance(conf);
		job.setJarByClass(HDF5.class);

		job.setOutputKeyClass(Text.class); // same filename as input
		job.setOutputValueClass(Text.class);// scanned attributes from *.h5

		job.setMapperClass(Map.class); // mapper only no reducer for now

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
		return 0;
	}

	public static class Map extends Mapper<Text, BytesWritable, Text, Text> {

		protected void setup(Context context) throws IOException {
			// log where mapper is executed
			hostname = InetAddress.getLocalHost().getHostName();
		}

		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {

			String filename = key.toString();
			byte[] bytes = value.getBytes();
			Text outputkey = new Text(key);
			Text outputvalue = new Text();
			List<String> attributes = new ArrayList<String>();

			if (!filename.toLowerCase().matches(".*h5.*")) {
				logger.error("unsupported HDF5 formats for input: " + filename);
				System.exit(1);
			}

			NetcdfFile netCDFfile = NetcdfFile.openInMemory("inmemory.h5", bytes);
			switch (context.getConfiguration().get("attr")) {
			case "tempo":
				attributes = get_tempo(netCDFfile);
				break;
			case "artist":
				attributes = get_artist_name(netCDFfile);
				break;
			case "simiar_artist":
				attributes = get_similar_artists(netCDFfile);
				break;
			case "id":
				attributes = get_song_id(netCDFfile);
				break;
			default:
				logger.error("Specified attribute " + attr + " is not supported now");
				break;
			}

			for (String attribute : attributes) {
				outputkey.set(filename + SEPARATOR);
				outputvalue.set(attribute);
				context.write(outputkey, outputvalue);
			}
		}
	}

	static List<String> get_tempo(NetcdfFile nc) throws IOException {
		List<String> temp = new ArrayList<String>();
		Variable var = (Variable) nc.findVariable("/analysis/songs.tempo");
		Array content = var.read();// 1D array
		temp.add(content.toString());
		return temp;
	}

	static List<String> get_artist_name(NetcdfFile nc) throws IOException {
		List<String> artists = new ArrayList<String>();
		Variable var = (Variable) nc.findVariable("/metadata/songs.artist_name");
		Array content = var.read();// 1D array
		artists.add(content.toString());
		return artists;
	}

	static List<String> get_similar_artists(NetcdfFile nc) throws IOException {
		List<String> simiar_artists = new ArrayList<String>();
		Variable var = (Variable) nc.findVariable("/metadata.similar_artists");
		Array content = var.read();// 1D array
		simiar_artists.add(content.toString());
		return simiar_artists;
	}

	static List<String> get_song_id(NetcdfFile nc) throws IOException {
		List<String> id = new ArrayList<String>();
		Variable var = (Variable) nc.findVariable("/metadata/songs.song_id");
		Array content = var.read();// 1D array
		id.add(content.toString());
		return id;
	}
	// TODO: other attributes like release, hotness, key, duration, artist
	// locations.
}
