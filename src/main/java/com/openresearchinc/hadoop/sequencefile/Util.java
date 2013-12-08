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

public class Util {

	private static Configuration conf = new Configuration();
	private final static Logger logger = Logger.getLogger(Util.class);

	public static void main(String[] args) throws Exception {
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length == 3) {
			CompressionCodec codec = null;
			switch (otherArgs[2].toLowerCase()) {
			case "default":
				codec = new DefaultCodec();
				break;
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
				codec = new GzipCodec();
			}
			Util.writeToSequenceFile(otherArgs[0], otherArgs[1], codec);
		} else if (otherArgs.length == 1) {
			Util.readSequenceFile(otherArgs[0]);
		} else {
			System.err
					.println("Usage: hadoop jar ./target/*.jar com.openresearchinc.hadoop.sequencefile.Util <input-file|dir> <output-file|dir> <default|gzip|bz2|snappy>");
			System.err
					.println("or: hadoop jar ./target/*.jar com.openresearchinc.hadoop.sequencefile.Util <file|dir-for-sequencefiles>");

			System.exit(2);
		}
	}

	public static void writeToSequenceFile(String inputURI, String outputURI,
			CompressionCodec codec) throws IOException {

		Path path = null;
		String inputFile = null;
		if (inputURI.startsWith("file://")) {
			inputFile = inputURI.substring(7, inputURI.length());
		} else if (inputURI.startsWith("s3://")) {
			System.exit(2); // TODO
		} else if (inputURI.startsWith("hdfs://")) {
			System.exit(2); // TODO
		} else {
			System.exit(2); // TODO
		}

		if (outputURI.startsWith("hdfs://")) {
			if (!conf.get("fs.defaultFS").contains("hdfs://")) {
				conf.set("fs.defaultFS", "hdfs://" + outputURI.split("/")[2]);
			}// only useful in eclipse, no need if running hadoop jar
			path = new Path(outputURI.replaceAll("hdfs://[a-z\\.\\:0-9]+", ""));
		} else if (outputURI.startsWith("file://")) {
			conf.set("fs.defaultFS", "file://" + outputURI.split("/")[2]);
			path = new Path(outputURI.replaceAll("file://", ""));
		} else if (outputURI.startsWith("s3://")) {
			System.exit(2); // TODO
		} else if (outputURI.startsWith("file://")) {
			System.exit(2); // TODO
		} else {
			System.exit(2); // TODO
		}

		File directory = new File(inputFile);
		if (!directory.exists())
			return;

		File[] dataFiles;
		if (directory.isFile()) {// if a single file
			dataFiles = new File[1];
			dataFiles[0] = directory;
			path = path.getParent();
		} else {// if directory, include all sub-dir and files underneath it
			dataFiles = directory.listFiles();
		}
		for (int i = 0; i < dataFiles.length; ++i) {
			File dataFile = dataFiles[i];

			Path destpath = new Path(path + "/" + dataFile.getName() + ".seq");
			SequenceFile.Writer writer = SequenceFile.createWriter(conf,
					SequenceFile.Writer.file(destpath), SequenceFile.Writer
							.compression(CompressionType.RECORD, codec),
					SequenceFile.Writer.keyClass(Text.class),
					SequenceFile.Writer.valueClass(BytesWritable.class));

			byte[] binary = FileUtils.readFileToByteArray(dataFile);
			// use absolute path as key
			Text key = new Text(dataFile.getAbsolutePath());
			// use binary content as value
			BytesWritable value = new BytesWritable(binary);
			writer.append(key, value);
			IOUtils.closeStream(writer);
		}
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
		} else if (sequenceFileURI.startsWith("file://")) {
			path = new Path(sequenceFileURI.replaceAll("file://", ""));
		} else if (sequenceFileURI.startsWith("s3://")) {
			System.exit(2); // TODO
		} else if (sequenceFileURI.startsWith("file://")) {
			System.exit(2); // TODO
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
