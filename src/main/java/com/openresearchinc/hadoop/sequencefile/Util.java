package com.openresearchinc.hadoop.sequencefile;

//Original idea and code from:
//1. http://stuartsierra.com/2008/04/24/a-million-little-files
//2. http://noushinb.blogspot.com/2013/04/reading-writing-hadoop-sequence-files.html 
//
//Planned enhancements include: 
//1. treating all input as binary, regardless it is text or binary
//2. include file from http://, s3://(AWS cloud storage),....
//3. include more compression modules: bz2, snappy, ... 

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
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
import org.slf4j.Logger;
import org.xml.sax.InputSource;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.io.ByteStreams;

//@formatter:off
/**
 * export LIBJARS=/path/jar1,/path/jar2 
 * export LIBJARS=~/.m2/repository/com/amazonaws/aws-java-sdk/1.0.002/aws-java-sdk-1.0.002.jar 
 * export HADOOP_CLASSPATH=`echo ${LIBJARS} | sed s/,/:/g`
 * $hadoop jar <path>/bin2seq.jar com.openresearchinc.hadoop.sequencefile.Util -in <inuri> -out <outuri> -codec <default|gzip|bz2|snappy> -libjars $LIBJARS"
 * 
 * @author heq 
 */
// @formatter:on

public class Util {
	final static Logger logger = org.slf4j.LoggerFactory.getLogger(Util.class);

	final static Configuration conf = new Configuration();
	final static AmazonS3 s3Client = new AmazonS3Client(new BasicAWSCredentials(System.getenv("AWS_ACCESS_KEY"),
			System.getenv("AWS_SECRET_KEY")), new ClientConfiguration());

	public static void main(String[] args) throws Exception {
		String usage = "Usage: hadoop jar ./target/bin2seq*.jar com.openresearchinc.hadoop.sequencefile.Util -pack -in <input-uri> -ext <ext> -out <output-uri> -codec <gzip|bz2|snappy>";
		String inputURI = null, outputURI = null, codec = null;
		CompressionCodec compression = null;
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		List<String> argList = Arrays.asList(otherArgs);
		int pos = argList.indexOf("-in");
		if (pos == -1) {
			System.err.println(usage);
			System.exit(2);
		}
		inputURI = otherArgs[pos + 1];

		pos = argList.indexOf("-out");
		if (pos == -1) {
			System.err.println(usage);
			System.exit(2);
		}
		outputURI = otherArgs[pos + 1];
		if (pos == -1) {
			System.err.println(usage);
			System.exit(2);
		}

		pos = argList.indexOf("-codec");
		if (pos == -1) {
			System.err.println(usage);
			System.exit(2);
		}
		codec = otherArgs[pos + 1];
		switch (codec.toLowerCase()) {
		case "gzip":
			compression = new GzipCodec();
			break;
		case "bz2":
			compression = new BZip2Codec();
			break;
		case "snappy":
			compression = new SnappyCodec();
			break;
		case "default":
			compression = new DefaultCodec();
			break;
		default:
			System.err.println(usage);
			System.exit(2);
		}

		int packPos = argList.indexOf("-pack");
		int extPos = argList.indexOf("-ext");
		if (packPos != -1 && extPos != -1 && inputURI.startsWith("s3")) {
			String ext = otherArgs[extPos + 1];
			logger.info("ext="+ext);
			crushFilesS3ToHDFS(inputURI, getHadoopMasterURI()+outputURI, ext, compression);
		} else {
			Pattern allfiles = Pattern.compile("/\\*.[A-Za-z0-9]+$");
			Matcher matcher = allfiles.matcher(inputURI);
			if (matcher.find()) {
				String ext = FilenameUtils.getExtension(inputURI);
				String path = FilenameUtils.getFullPathNoEndSeparator(inputURI);
				List<String> URIs = listFiles(path, ext);
				for (String uri : URIs) {
					String filename = new File(uri).getName();
					Util.writeToSequenceFile(uri, outputURI + "/" + filename + ".seq", compression);
				}
			} else {
				String filename = new File(inputURI).getName();
				Util.writeToSequenceFile(inputURI, outputURI + "/" + filename + ".seq", compression);
			}
		}
	}

	/**
	 * Return Hadoop Cluster Master URI in the format of hdfs://namenode-ip:port
	 * 
	 * @return
	 */
	public static String getHadoopMasterURI() {
		HashMap<String, String> hm = parseHadoopConf();
		return hm.get("fs.default.name");
	}

	/**
	 * Return HDFS block size (to maximize data locality)
	 * 
	 * @return
	 */
	public static int getHDFSBlockSize() {
		int size = 64 * 1024 * 1024; // default Apache Hadoop HDFS block size
		HashMap<String, String> hm = parseHadoopConf();
		String mb = hm.get("dfs.block.size"); // size in MB
		if (org.apache.commons.lang3.StringUtils.containsIgnoreCase(mb, "MB")) {
			size = Integer.parseInt(mb.replaceAll("[^\\d.]", ""));
		} else {
			logger.warn("dfs.block.size in core-site.xml is not specified or not in 'MB', use default 64MB HDFS block size");
		}
		return size;
	}

	static HashMap<String, String> parseHadoopConf() {
		HashMap<String, String> hm = new HashMap<String, String>();

		File hadoopHomeDir = new File(System.getenv("HADOOP_HOME") + "/etc/hadoop");
		if (hadoopHomeDir.isDirectory()) {
			File[] files = hadoopHomeDir.listFiles(new FileFilter() {
				public boolean accept(File file) {
					return file.getName().endsWith("core-site.xml");
				}
			});
			if (files.length == 1) {
				try {
					XPathFactory xpf = XPathFactory.newInstance();
					XPath xpath = xpf.newXPath();

					XPathExpression xpe = xpath.compile("//property[name/text()='fs.default.name']/value");
					InputSource coresitexml = new InputSource(new FileReader(files[0]));
					hm.put("fs.default.name", xpe.evaluate(coresitexml));

					xpe = xpath.compile("//property[name/text()='dfs.block.size']/value");
					coresitexml = new InputSource(new FileReader(files[0]));
					hm.put("dfs.block.size", xpe.evaluate(coresitexml));
				} catch (IOException ioe) {
					logger.error("Cannot find core-site.xml under $HADOOP_HOME");
					System.exit(1);
				} catch (XPathExpressionException e) {
					logger.error("Error when parsing core-site.xml under $HADOOP_HOME");
					System.exit(1);
				}
			} else {
				logger.error("Cannot find fs.default.name in core-site.xml");
				System.exit(1);
			}
		} else {
			logger.warn("$HADOOP_HOME is NOT defeined");
		}
		return hm;
	}

	public static void crushFilesS3ToHDFS(String s3URI, String hdfsDir, String ext, CompressionCodec codec)
			throws IOException {
		Text key;
		BytesWritable value;

		conf.set("fs.defaultFS", getHadoopMasterURI());
		Path outpath;
		if (hdfsDir.startsWith("hdfs://"))
			outpath = new Path(hdfsDir.replaceAll("hdfs://[a-z\\.\\:0-9]+", ""));
		else
			outpath = new Path(hdfsDir); // TODO nn:port is required for now

		// String bucket = s3URI.split("/")[2];
		List<String> argsList = new LinkedList<String>(Arrays.asList(s3URI.split("/")));
		String bucket = argsList.get(2);
		argsList.remove(0);
		argsList.remove(0);
		argsList.remove(0);// trimming leading protocol and bucket
		String prefix = StringUtils.join(argsList, "/");
		ObjectListing listing = s3Client.listObjects(bucket, prefix);
		// list all objects recursively under bucket/prefix
		List<S3ObjectSummary> summaries = listing.getObjectSummaries();
		while (listing.isTruncated()) {// only if there are 1000+ objects
			listing = s3Client.listNextBatchOfObjects(listing);
			summaries.addAll(listing.getObjectSummaries());
		}

		int seq = 1;
		int ballsize = 0; // total size of files packed into one sequencefile
		int boxsize = getHDFSBlockSize();// 128MB EMR HDFS default block size to
											// max locality

		SequenceFile.Writer writer = SequenceFile.createWriter(conf,
				SequenceFile.Writer.file(new Path(outpath + "/" + seq + ".seq")),
				SequenceFile.Writer.compression(CompressionType.RECORD, codec),
				SequenceFile.Writer.keyClass(Text.class), SequenceFile.Writer.valueClass(BytesWritable.class));

		for (S3ObjectSummary summary : summaries) {
			if (!summary.getKey().endsWith(ext))
				continue;// only pack files with specified extension
			S3Object s3object = s3Client.getObject(summary.getBucketName(), summary.getKey());
			InputStream objectContent = s3object.getObjectContent();
			// byte[] bytes
			// =org.apache.commons.io.IOUtils.toByteArray(objectContent);
			byte[] bytes = ByteStreams.toByteArray(objectContent);
			// byte[] bytes=readLargeData(objectContent);
			objectContent.close();
			byte[] uncompressed;

			if (summary.getKey().endsWith("bz2")) {
				uncompressed = CompressUtil.unBZip2(bytes);
			} else {
				uncompressed = bytes;
			}// TODO other compressors like gzip,
			key = new Text(summary.getBucketName() + summary.getKey());
			value = new BytesWritable(uncompressed);
			if (ballsize > boxsize) {
				IOUtils.closeStream(writer);
				writer = SequenceFile.createWriter(conf,
						SequenceFile.Writer.file(new Path(outpath + "/" + seq + ".seq")),
						SequenceFile.Writer.compression(CompressionType.RECORD, codec),
						SequenceFile.Writer.keyClass(Text.class), SequenceFile.Writer.valueClass(BytesWritable.class));
				++seq;
				ballsize = 0;
			} else {
				ballsize += uncompressed.length;
			}
			writer.append(key, value);
		}
		IOUtils.closeStream(writer);
	}

	static byte[] readLargeData(InputStream is) throws IOException {
		byte[] bytes = new byte[is.available()];
		int offset = 0;
		int numRead = 0;
		while (offset < bytes.length && (numRead = is.read(bytes, offset, bytes.length - offset)) >= 0) {
			offset += numRead;
		}
		if (offset < bytes.length) {
			throw new IOException("Could not completely read file ");
		}
		return bytes;
	}

	public static List<String> listFiles(String dir, String ext) throws Exception {
		List<String> uri = new ArrayList<String>();

		if (dir.startsWith("s3://") || dir.startsWith("s3n://")) {
			String[] args = dir.split("/");
			String bucket = args[2].split("\\.")[0];
			List<String> argsList = new LinkedList<String>(Arrays.asList(args));
			argsList.remove(0);
			argsList.remove(0);
			argsList.remove(0);// trimming leading protocol and bucket
			String prefix = StringUtils.join(argsList, "/");

			ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(bucket).withPrefix(prefix);
			ObjectListing objectListing;

			do {
				objectListing = s3Client.listObjects(listObjectsRequest);
				for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
					if (!objectSummary.getKey().endsWith(ext))
						continue;
					uri.add("s3n://" + bucket + "/" + objectSummary.getKey());
					if (logger.isDebugEnabled())
						logger.debug(" - " + objectSummary.getKey() + "  " + "(size = " + objectSummary.getSize() + ")");
				}
				listObjectsRequest.setMarker(objectListing.getNextMarker());
			} while (objectListing.isTruncated());
		} else if (dir.startsWith("hdfs://")) {
			FileSystem hdfs = FileSystem.get(new URI(dir), conf);
			FileStatus[] fileStatus = hdfs.listStatus(new Path(dir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			for (Path path : paths) {
				if (path.getName().endsWith(ext)) {
					uri.add(path.toString());
				}
			}
		}// TODO: other sources
		return uri;
	}

	public static void writeToSequenceFile(String inputURI, String outputURI, CompressionCodec codec)
			throws IOException, NoSuchAlgorithmException {
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

			byte[] uncompressed = null;
			if (inputURI.endsWith("bz2")) {
				uncompressed = CompressUtil.unBZip2(bytes);
			} else {
				uncompressed = bytes;
			}
			key = new Text(dataFile.getAbsolutePath());
			value = new BytesWritable(uncompressed);
		} else if (inputURI.startsWith("s3://") || inputURI.startsWith("s3n://")) {
			String[] args = inputURI.split("/");
			String bucket = args[2].split("\\.")[0];
			List<String> argsList = new LinkedList<String>(Arrays.asList(args));
			argsList.remove(0);
			argsList.remove(0);
			argsList.remove(0);// trimming leading protocol and bucket
			String object = StringUtils.join(argsList, "/");
			GetObjectRequest request = new GetObjectRequest(bucket, object);
			S3Object s3object = s3Client.getObject(request);
			InputStream objectContent = s3object.getObjectContent();
			bytes = org.apache.commons.io.IOUtils.toByteArray(objectContent);
			objectContent.close();

			byte[] uncompressed = null;
			if (inputURI.endsWith("bz2")) {
				uncompressed = CompressUtil.unBZip2(bytes);
			} else {
				uncompressed = bytes;
			}
			key = new Text(inputURI);
			value = new BytesWritable(uncompressed);
		} else if (inputURI.startsWith("http://")) {
			String[] args = inputURI.split("/");
			String host = args[2];
			String uri = inputURI.replaceAll("http://[a-zA-Z0-9-.]+", "");
			InputStream in = new BufferedInputStream(new URL("http", host, uri).openStream());
			key = new Text(inputURI);
			bytes = org.apache.commons.io.IOUtils.toByteArray(in);
			value = new BytesWritable(bytes);

		} else {
			logger.error("File system option have not been implemented yet");
			System.exit(2); // TODO
		}

		if (outputURI.startsWith("hdfs://")) {
			// assume default path is HDFS
			if (!conf.get("fs.defaultFS").contains("hdfs://")) {
				conf.set("fs.defaultFS", "hdfs://" + outputURI.split("/")[2]);
			}// only useful in eclipse, no need if running hadoop jar
			outpath = new Path(outputURI.replaceAll("hdfs://[a-z\\.\\:0-9]+", ""));
		} else if (outputURI.startsWith("s3n://")) {
			conf.set("fs.s3n.awsAccessKeyId", System.getenv("AWS_ACCESS_KEY"));
			conf.set("fs.s3n.awsSecretAccessKey", System.getenv("AWS_SECRET_KEY"));
			outpath = new Path(outputURI);
		} else if (outputURI.startsWith("file://")) {
			outpath = new Path(outputURI.replaceAll("file://", ""));
		} else {
			logger.error("File system option has not been implemented yet");
			System.exit(2); // TODO
		}

		SequenceFile.Writer writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(outpath),
				SequenceFile.Writer.compression(CompressionType.RECORD, codec),
				SequenceFile.Writer.keyClass(Text.class), SequenceFile.Writer.valueClass(BytesWritable.class));
		writer.append(key, value);
		IOUtils.closeStream(writer);
	}

	public static void listSequenceFileKeys(String sequenceFileURI) throws Exception {
		Path path = null;
		if (sequenceFileURI.startsWith("hdfs://")) {
			if (!conf.get("fs.defaultFS").contains("hdfs://")) {
				conf.set("fs.defaultFS", "hdfs://" + sequenceFileURI.split("/")[2]);
			}// only useful in eclipse, no need if running hadoop jar
			path = new Path(sequenceFileURI.replaceAll("hdfs://[a-z\\.\\:0-9]+", ""));
		} else if (sequenceFileURI.startsWith("s3n://")) {
			conf.set("fs.s3n.awsAccessKeyId", System.getenv("AWS_ACCESS_KEY"));
			conf.set("fs.s3n.awsSecretAccessKey", System.getenv("AWS_SECRET_KEY"));
			path = new Path(sequenceFileURI); // NEXT SPIKE
		} else if (sequenceFileURI.startsWith("file://")) {
			path = new Path(sequenceFileURI.replaceAll("file://", ""));
		} else {
			logger.error("File system option have not been implemented yet");
			System.exit(2); // TODO
		}

		SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));
		Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		while (reader.next(key)) {
			logger.info("key : " + key.toString());
		}
		IOUtils.closeStream(reader);
	}

	public static byte[] readSequenceFileFromS3(String s3URI) throws IOException {
		String accessKey = System.getenv("AWS_ACCESS_KEY");
		String secretKey = System.getenv("AWS_SECRET_KEY");
		if (accessKey == null || accessKey.isEmpty() || secretKey == null || secretKey.isEmpty()) {
			logger.error("$AWS_ACCESS_KEY or $AWS_SECRET_KEY is not set");
			System.exit(1);
		}
		Pattern pattern = Pattern.compile("^s3n://\\S+/\\S+");
		Matcher m = pattern.matcher(s3URI);
		if (!m.find()) {
			logger.error("Wrong S3 URI format, should be something like s3n://bucket/object");
			System.exit(1);
		}
		conf.set("fs.s3n.awsAccessKeyId", accessKey);
		conf.set("fs.s3n.awsSecretAccessKey", secretKey);
		return read(new Path(s3URI));
	}

	public static byte[] readSequenceFileFromHDFS(String hdfsURI) throws IOException {
		Pattern pattern = Pattern.compile("^hdfs://\\S+:\\d+/\\S+");
		Matcher m = pattern.matcher(hdfsURI);
		if (!m.find()) {
			logger.error("Wrong HDFS URI format, should be something like hdfs://namenod-ip:port/object");
			System.exit(1);
		}
		return read(new Path(hdfsURI));
	}

	public static byte[] readSequenceFileFromFS(String fileURI) throws IOException {
		Pattern pattern = Pattern.compile("^file://\\S+/\\S+");
		Matcher m = pattern.matcher(fileURI);
		if (!m.find()) {
			logger.error("Wrong Native File System URI format, should be something like file:///path/.../file");
		}
		return read(new Path(fileURI.replaceAll("file://", "")));
	}

	private static byte[] read(Path path) throws IOException {
		// Map<Text, byte[]> map = new HashMap<Text, byte[]>();
		SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));
		Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		BytesWritable value = (BytesWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
		while (reader.next(key, value)) {
			if (logger.isDebugEnabled())
				logger.debug("key : " + key.toString() + " - value size: " + value.getBytes().length);
			return value.getBytes();
		}
		IOUtils.closeStream(reader);
		return null;
	}

	// @Deprecated
	public static Map<Text, byte[]> readSequenceFile(String sequenceFileURI) throws IOException {
		Map<Text, byte[]> map = new HashMap<Text, byte[]>();
		Path path = null;
		if (sequenceFileURI.startsWith("hdfs://")) {
			if (!conf.get("fs.defaultFS").contains("hdfs://")) {
				conf.set("fs.defaultFS", "hdfs://" + sequenceFileURI.split("/")[2]);
			}// only useful in eclipse, no need if running hadoop jar
			path = new Path(sequenceFileURI.replaceAll("hdfs://[a-z\\.\\:0-9]+", ""));
		} else if (sequenceFileURI.startsWith("s3n://") || sequenceFileURI.startsWith("s3://")) {
			conf.set("fs.s3n.awsAccessKeyId", System.getenv("AWS_ACCESS_KEY"));
			conf.set("fs.s3n.awsSecretAccessKey", System.getenv("AWS_SECRET_KEY"));
			path = new Path(sequenceFileURI);
		} else if (sequenceFileURI.startsWith("file://")) {
			path = new Path(sequenceFileURI.replaceAll("file://", ""));
		} else {
			logger.error("File system option have not been implemented yet");
			System.exit(2); // TODO
		}

		SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));
		Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		BytesWritable value = (BytesWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
		while (reader.next(key, value)) {
			if (logger.isDebugEnabled())
				logger.debug("key : " + key.toString() + " - value size: " + value.getBytes().length);
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
	public static void copySequenceFile(String from, String to, String remoteHadoopFS) throws IOException {
		conf.set("fs.defaultFS", remoteHadoopFS);
		FileSystem fs = FileSystem.get(conf);

		Path localPath = new Path(from);
		Path hdfsPath = new Path(to);
		boolean deleteSource = true;

		fs.copyFromLocalFile(deleteSource, localPath, hdfsPath);
		logger.info("Copied SequenceFile from: " + from + " to: " + to);
	}
}
