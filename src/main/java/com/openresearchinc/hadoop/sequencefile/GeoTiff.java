package com.openresearchinc.hadoop.sequencefile;

import java.io.IOException;
import java.net.InetAddress;

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
import org.gdal.gdal.Dataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//@formatter:off
/**
 * 
 * A program to demo retrive attributes from Geotiff images as Hadoop SequenceFile stored on hdfs:// or s3://
 *  
 *  
 * @author heq
 */
// @formatter:on

public class GeoTiff extends Configured implements Tool {
	final static Logger logger = LoggerFactory.getLogger(GeoTiff.class);
	static String hostname = "localhost";

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new GeoTiff(), args);
		System.exit(res);
	}

	@Override
	public final int run(final String[] args) throws Exception {
		final String usage = "Usage: hadoop jar ./target/bin2seq*.jar com.openresearchinc.hadoop.sequencefile.GeoTiff  "
				+ "-libs $LIBJARS  <input URI of * tif.seq on HDFS-or-s3> <output-uri-of-files> ";

		Job job = Job.getInstance(super.getConf());
		job.setJarByClass(GeoTiff.class);

		job.setOutputKeyClass(Text.class); // same filename as input
		job.setOutputValueClass(Text.class);// scanned attributes from *.tif.seq

		job.setMapperClass(Map.class); // mapper only no reducer for now

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputDirRecursive(job, true); //add data recursively
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
		return 0;
	}

	public static class Map extends Mapper<Text, BytesWritable, Text, Text> {

		protected void setup(Context context) throws IOException {
			hostname = InetAddress.getLocalHost().getHostName();
			logger.debug("host={}", hostname);
			org.gdal.gdal.gdal.AllRegister();
		}

		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
			String filename = key.toString();
			byte[] bytes = value.getBytes();
			Text outputkey = new Text();
			Text outputvalue = new Text();

			if (!filename.toLowerCase().matches(".*tif.*")) {
				logger.error("unsupported NetCDF formats for input: " + filename);
				System.exit(1);
			}
			org.gdal.gdal.gdal.FileFromMemBuffer("/vsimem/geotiffinmem", bytes);
			Dataset dataset = org.gdal.gdal.gdal.Open("/vsimem/geotiffinmem");
			outputkey.set(filename);

			outputvalue.set(getCoordinatesodCorners(dataset));
			context.write(outputkey, outputvalue);
		}
	}

	// A demo to retrieve CooridnateCorners of a GeoTiff
	static String getCoordinatesodCorners(org.gdal.gdal.Dataset dataset) {
		int width = dataset.GetRasterXSize();
		int height = dataset.GetRasterYSize();
		double[] gt = dataset.GetGeoTransform();
		double minx = gt[0];
		double miny = gt[3] + width * gt[4] + height * gt[5];
		double maxx = gt[0] + width * gt[1] + height * gt[2];
		double maxy = gt[3];

		return minx + "," + maxx + "," + miny + "," + maxy;
	}
}
