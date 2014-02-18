package com.openresearchinc.hadoop.sequencefile;

//Original idea and code from:
//1. http://stuartsierra.com/2008/04/24/a-million-little-files
//2. http://noushinb.blogspot.com/2013/04/reading-writing-hadoop-sequence-files.html 
//
//Planned enhancements include: 
//1. treating all input as binary, regardless it is text or binary
//2. include file from http://, s3://(AWS cloud storage),....
//3. include more compression modules: bz2, snappy, ... 

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public class Util {

	private static Configuration conf = new Configuration();
	private final static Logger logger = Logger.getLogger(Util.class);

	public static void main(String[] args) throws Exception {
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err
					.println("Usage: hadoop jar ./target/*.jar com.openresearchinc.hadoop.sequencefile.Util <input-uri> <output-uri> <gzip|bz2|snappy>");
			System.exit(2);
		}
		CompressionCodec codec = null;
		switch (otherArgs[2].toLowerCase()) {
		case "gzip":
			codec = new GzipCodec();
			break;
		case "bz2":
			codec = new BZip2Codec();
			break;
		case "snappy":
			codec = new SnappyCodec();
			break;
		default:
			codec = new DefaultCodec();
		}
		Util.writeToSequenceFile(otherArgs[0], otherArgs[1], codec);
	}

	public static void writeToSequenceFile(String inputURI, String outputURI,
			CompressionCodec codec) throws IOException {
		Path outpath = null;
		String inputFile = null;
		byte[] bytes = null;
		Text key = null;
		BytesWritable value = null;

		if (inputURI.startsWith("file://")) {
			inputFile = inputURI.substring(7, inputURI.length());
			File dataFile = new File(inputFile);
			if (!dataFile.exists())
				return;
			bytes = FileUtils.readFileToByteArray(dataFile);
			key = new Text(dataFile.getAbsolutePath());
			value = new BytesWritable(bytes);
		} else if (inputURI.startsWith("s3://")) {
			String[] args = inputURI.split("/");
			String bucket = args[2].split("\\.")[0];
			String object = args[3];
			AmazonS3 s3Client = new AmazonS3Client(new BasicAWSCredentials(
					System.getenv("AWS_ACCESS_KEY_ID"),
					System.getenv("AWS_SECRET_KEY")));
			GetObjectRequest request = new GetObjectRequest(bucket, object);
			S3Object s3object = s3Client.getObject(request);
			InputStream objectContent = s3object.getObjectContent();
			bytes = org.apache.commons.io.IOUtils.toByteArray(objectContent);
			objectContent.close();
			key = new Text(inputURI);
			value = new BytesWritable(bytes);
		} else if (inputURI.startsWith("hdfs://")) {
			System.exit(2); // TODO
		} else {
			System.exit(2); // TODO
		}

		if (outputURI.startsWith("hdfs://")) {
			if (!conf.get("fs.defaultFS").contains("hdfs://")) {
				conf.set("fs.defaultFS", "hdfs://" + outputURI.split("/")[2]);
			}// only useful in eclipse, no need if running hadoop jar
			outpath = new Path(outputURI.replaceAll("hdfs://[a-z\\.\\:0-9]+",
					""));
		} else if (outputURI.startsWith("s3://")) {
			System.exit(2); // TODO
		} else if (outputURI.startsWith("file://")) {
			outpath = new Path(outputURI.replaceAll("file://", ""));
		} else {
			System.exit(2); // TODO
		}

		SequenceFile.Writer writer = SequenceFile.createWriter(conf,
				SequenceFile.Writer.file(outpath),
				SequenceFile.Writer.compression(CompressionType.RECORD, codec),
				SequenceFile.Writer.keyClass(Text.class),
				SequenceFile.Writer.valueClass(BytesWritable.class));
		writer.append(key, value);
		org.apache.hadoop.io.IOUtils.closeStream(writer);
	}

	public static void listSequenceFileKeys(String sequenceFileURI)
			throws Exception {
		Path path = null;
		if (sequenceFileURI.startsWith("hdfs://")) {
			if (!conf.get("fs.defaultFS").contains("hdfs://")) {
				conf.set("fs.defaultFS", "hdfs://"
						+ sequenceFileURI.split("/")[2]);
			}// only useful in eclipse, no need if running hadoop jar
			path = new Path(sequenceFileURI.replaceAll(
					"hdfs://[a-z\\.\\:0-9]+", ""));
		} else if (sequenceFileURI.startsWith("s3://")) {
			System.exit(2); // TODO
		} else if (sequenceFileURI.startsWith("file://")) {
			path = new Path(sequenceFileURI.replaceAll("file://", ""));
		} else {
			System.exit(2); // TODO
		}

		SequenceFile.Reader reader = new SequenceFile.Reader(conf,
				SequenceFile.Reader.file(path));
		Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(),
				conf);
		while (reader.next(key)) {
			logger.info("key : " + key.toString());
		}
		IOUtils.closeStream(reader);
	}

	public static Map<Text, byte[]> readSequenceFile(String sequenceFileURI)
			throws IOException {
		Map<Text, byte[]> map = new HashMap<Text, byte[]>();
		Path path = null;
		if (sequenceFileURI.startsWith("hdfs://")) {
			if (!conf.get("fs.defaultFS").contains("hdfs://")) {
				conf.set("fs.defaultFS", "hdfs://"
						+ sequenceFileURI.split("/")[2]);
			}// only useful in eclipse, no need if running hadoop jar
			path = new Path(sequenceFileURI.replaceAll(
					"hdfs://[a-z\\.\\:0-9]+", ""));
		} else if (sequenceFileURI.startsWith("s3://")) {
			System.exit(2); // TODO
		} else if (sequenceFileURI.startsWith("file://")) {
			path = new Path(sequenceFileURI.replaceAll("file://", ""));
		} else {
			System.exit(2); // TODO
		}

		SequenceFile.Reader reader = new SequenceFile.Reader(conf,
				SequenceFile.Reader.file(path));
		Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(),
				conf);
		BytesWritable value = (BytesWritable) ReflectionUtils.newInstance(
				reader.getValueClass(), conf);
		while (reader.next(key, value)) {
			logger.info("key : " + key.toString() + " - value size: "
					+ value.getBytes().length);
			map.put(key, value.getBytes());
		}
		IOUtils.closeStream(reader);
		return map;
	}

	/**
	 * Copy a local sequence file to a remote file on HDFS.
	 * 
	 * @param from
	 *            Name of the sequence file to copy
	 * @param to
	 *            Name of the sequence file to copy to
	 * @param remoteHadoopFS
	 *            HDFS host URI
	 * 
	 * @throws IOException
	 */
	public static void copySequenceFile(String from, String to,
			String remoteHadoopFS) throws IOException {
		conf.set("fs.defaultFS", remoteHadoopFS);
		FileSystem fs = FileSystem.get(conf);

		Path localPath = new Path(from);
		Path hdfsPath = new Path(to);
		boolean deleteSource = true;

		fs.copyFromLocalFile(deleteSource, localPath, hdfsPath);
		logger.info("Copied SequenceFile from: " + from + " to: " + to);
	}

	/**
	 * Print all the values in Hadoop HDFS configuration object.
	 * 
	 * @param conf
	 */
	public static void listHadoopConfiguration(Configuration conf) {
		int i = 0;
		logger.info("------------------------------------------------------------------------------------------");
		Iterator iterator = conf.iterator();
		while (iterator.hasNext()) {
			i++;
			iterator.next();
			logger.info(i + " - " + iterator.next());
		}
		logger.info("------------------------------------------------------------------------------------------");
	}
}
