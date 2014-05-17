package com.openresearchinc.hadoop.sequencefile;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Random;

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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ucar.ma2.ArrayDouble;
import ucar.ma2.ArrayFloat;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

//@formatter:off
/**
 * 
 * A program to demo a fair complex search (min/max/average) percipitation over NetCDF file (*.nc) encoded 
 * as Hadoop SequenceFile stored on hdfs:// or s3n://
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
public class NetCDF extends Configured implements Tool {
	final static Logger logger = LoggerFactory.getLogger(NetCDF.class);
	static String hostname = "localhost";

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new NetCDF(), args);
		System.exit(res);
	}

	@Override
	public final int run(final String[] args) throws Exception {
		final String usage = "Usage: hadoop jar ./target/bin2seq*.jar com.openresearchinc.hadoop.sequencefile.NetCDF  "
				+ "-libs $LIBJARS  <input-uri-h5-on-hdfs-or-s3> <output-uri-of-files-with-minmaxavgPercipitation> ";

		Job job = Job.getInstance(super.getConf());
		job.setJarByClass(NetCDF.class);

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
			Text outputkey = new Text();
			Text outputvalue = new Text();

			if (!filename.toLowerCase().matches(".*nc.*")) {
				logger.error("unsupported NetCDF formats for input: " + filename);
				System.exit(1);
			}

			NetcdfFile netCDFfile = NetcdfFile.openInMemory("inmemory.h5", bytes);
			outputkey.set(filename);
			try {
				outputvalue.set(getMinMaxAvgPrecipitationFromRegion(netCDFfile));
			} catch (InvalidRangeException e) {
				logger.warn("reading error for " + filename);
				outputvalue.set(new Text());
			}
			context.write(outputkey, outputvalue);
		}
	}

	// A demo computing routine to get min/max/average precipitation from in a
	// given region
	static String getMinMaxAvgPrecipitationFromRegion(NetcdfFile nc) throws InvalidRangeException, IOException {
		int SIZE = 100;
		Variable time = nc.findVariable("time");
		ArrayDouble.D1 days = (ArrayDouble.D1) time.read();
		Variable lat = nc.findVariable("lat");
		if (lat == null) {
			logger.error("Cannot find Variable latitude(lat)");
			System.exit(1);
		}
		ArrayFloat.D1 absolutelat = (ArrayFloat.D1) lat.read();
		Variable lon = nc.findVariable("lon");
		if (lon == null) {
			logger.error("Cannot find Variable longitude(lon)");
			System.exit(1);
		}
		ArrayFloat.D1 absolutelon = (ArrayFloat.D1) lon.read();
		Variable pres = nc.findVariable("pr");
		if (pres == null) {
			logger.error("Cannot find Variable precipitation(pr)");
			System.exit(1);
		}

		Random rand = new Random();
		int orig_lat = rand.nextInt((int) lat.getSize());
		orig_lat = Math.min(orig_lat, (int) (lat.getSize() - SIZE));
		int orig_lon = rand.nextInt((int) lon.getSize());
		orig_lon = Math.min(orig_lon, (int) (lon.getSize() - SIZE));

		int[] origin = new int[] { 0, orig_lat, orig_lon };
		int[] size = new int[] { 1, SIZE, SIZE };

		ArrayFloat.D3 data3D = (ArrayFloat.D3) pres.read(origin, size);
		double max = Double.NEGATIVE_INFINITY;
		double min = Double.POSITIVE_INFINITY;
		double sum = 0;
		for (int j = 0; j < SIZE; j++) {
			for (int k = 0; k < SIZE; k++) {
				double current = data3D.get(0, j, k);
				max = (current > max ? current : max);
				min = (current < min ? current : min);
				sum += current;
			}
		}

		if (logger.isDebugEnabled())
			logger.debug(hostname + "," + days + "," + absolutelat.get(orig_lat) + "," + absolutelon.get(orig_lon)
					+ "," + SIZE + ":" + min + "," + max + "," + sum / (SIZE * SIZE));

		return days + "," + absolutelat.get(orig_lat) + "," + absolutelon.get(orig_lon) + "," + SIZE + ":" + min + ","
				+ max + "," + (double) sum / (SIZE * SIZE);
	}
}
