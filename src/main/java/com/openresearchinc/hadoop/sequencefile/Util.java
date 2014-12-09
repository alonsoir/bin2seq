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
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tools.bzip2.CBZip2InputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

//@formatter:off
/**
 * $export LIBJARS=/path/jar1,/path/jar2 
 * $export HADOOP_CLASSPATH=`echo ${LIBJARS} | sed s/,/:/g` 
 * $hadoop jar <path>/bin2seq.jar com.openresearchinc.hadoop.sequencefile.Util -libjars $LIBJARS [-pack] -in  <inuri> -out <outuri> -codec <default|gzip|bz2|snappy>"
 * 
 * @author heq
 */
// @formatter:on

public class Util {
	final static Logger logger = LoggerFactory.getLogger(Util.class);

	final static Configuration conf = new Configuration();
	final static AmazonS3 s3Client = new AmazonS3Client(new BasicAWSCredentials(System.getenv("AWS_ACCESS_KEY"),
			System.getenv("AWS_SECRET_KEY")), new ClientConfiguration());

	public static void main(String[] args) throws Exception {
		String usage = "Usage: hadoop jar ./target/bin2seq*.jar com.openresearchinc.hadoop.sequencefile.Util -list -in <input-uri> -ext <ext> -out <output-uri> -codec <none|default|gzip|bz2|snappy>";
		String inputURI = null, outputURI = null, codec = null;
		CompressionCodec compression = null;
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		List<String> argList = Arrays.asList(otherArgs);
		int pos;
		if ((pos = argList.indexOf("-in")) != -1)
			inputURI = otherArgs[pos + 1];

		if ((pos = argList.indexOf("-out")) != -1)
			outputURI = otherArgs[pos + 1];

		if ((pos = argList.indexOf("-codec")) != -1) {
			codec = otherArgs[pos + 1];
			switch (codec.toLowerCase()) {
			case "none":
				compression = null;
				break;
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
		}

		if (inputURI != null && argList.indexOf("-list") != -1) {
			listSequenceFileKeys(inputURI);
			System.exit(0);
		}
		if (inputURI.startsWith("s3://") && (outputURI.startsWith("hdfs://") || outputURI.startsWith("s3://"))
				&& (pos = argList.indexOf("-ext")) != -1) {
			String ext = otherArgs[pos + 1];
			logger.info("input ={},outputURI={}, ext={}", inputURI, outputURI, ext);
			copyEncodeFilesFromS3ToHDFSS3(inputURI, outputURI, ext, compression);
			System.exit(0);
		}

		if (inputURI != null && outputURI != null && codec != null) {
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
		} else {
			System.err.println(usage);
			System.exit(1);
		}
	}

	/**
	 * Return Hadoop Cluster Master URI in the format of hdfs://namenode-ip:port
	 * 
	 * @return
	 */
	public static String getHadoopMasterURI() {
		return parseHadoopConf("core-site.xml", "fs.default.name");
	}

	/**
	 * Return HDFS block size (to maximize data locality)
	 * 
	 * @return
	 */
	public static int getHDFSBlockSize() {
		int size = 64 * 1024 * 1024; // default Apache Hadoop HDFS block size
		String mb = parseHadoopConf("hdfs-site.xml", "dfs.block.size");
		if (mb.toUpperCase().contains("MB")) {
			size = Integer.parseInt(mb.replaceAll("[^\\d.]", ""));
		} else if (mb.matches("\\d+")) {
			size = Integer.parseInt(mb);
		} else {
			logger.warn("dfs.block.size in hdfs-site.xml is not specified or not in 'MB', use default 64MB HDFS block size");
		}
		return size;
	}

	/**
	 * Parse Hadoop configuration to obtain cluster-specific parameters, e.g. master/NameNode, default HDFS block
	 * size,...
	 * 
	 * @param hadoopconf
	 * @param key
	 * @return
	 */
	static String parseHadoopConf(String hadoopconf, String key) {
		File hadoopHomeDir = new File(System.getenv("HADOOP_HOME") + "/etc/hadoop");
		try {
			XPathFactory xpf = XPathFactory.newInstance();
			XPath xpath = xpf.newXPath();
			XPathExpression xpe = xpath.compile("//property[name/text()='" + key + "']/value");
			InputSource coresitexml = new InputSource(new FileReader(hadoopHomeDir + "/" + hadoopconf));
			return xpe.evaluate(coresitexml);
		} catch (IOException ioe) {
			logger.error("Cannot find " + hadoopconf + " under $HADOOP_HOME");
			System.exit(1);
		} catch (XPathExpressionException e) {
			logger.error("Error when parsing" + hadoopconf + " under $HADOOP_HOME");
			System.exit(1);
		}
		return null;
	}

	/**
	 * Copy file on AWS s3 recursively to Hadoop HDFS, keep the file structure the same as S3.
	 * 
	 * @param s3URI
	 *            , e.g., s3://object/key or s3n://object/key
	 * @param hdfsDir
	 *            , e.g. hdfs://master:port/path/file
	 * @param ext
	 *            e.g., "*.gz"
	 * @param codec
	 *            e.g., snappyCodec
	 * @throws IOException
	 */
	public static void copyEncodeFilesFromS3ToHDFSS3(String s3URI, String outputDir, String ext, CompressionCodec codec)
			throws IOException {
		String masterURL = getHadoopMasterURI();
		conf.set("fs.defaultFS", masterURL);

		Path outpath = null;
		if (outputDir.startsWith("hdfs://"))
			outpath = new Path(masterURL + outputDir.replaceAll("^hdfs:/{2,}", "/"));
		else if (outputDir.startsWith("s3://") || outputDir.startsWith("s3n://")) {
			outpath = new Path(outputDir);
		} else {
			throw new IOException("unsupported output format");
		}
		logger.debug("HDFS Output dir={}", outputDir);

		String trimmedS3URI = s3URI.replaceAll("^s3[n]?:/{2,}", ""); //trim protocol part
		String bucket = StringUtils.substringBefore(trimmedS3URI, "/"); //the first as bucket
		String prefix = StringUtils.substringAfter(trimmedS3URI, "/"); //the rest are prefix

		ObjectListing listing = s3Client.listObjects(bucket, prefix);
		// list all objects recursively under bucket/prefix
		List<S3ObjectSummary> summaries = listing.getObjectSummaries();
		while (listing.isTruncated()) {// only if there are 1000+ objects
			listing = s3Client.listNextBatchOfObjects(listing);
			summaries.addAll(listing.getObjectSummaries());
		}
		logger.debug("file #={}", summaries.size());

		int seq = 1; //starting number of packed sequence File 1.seq, 2.seq,...
		SequenceFile.Writer writer = createSequenceFileWriter(outpath + "/" + seq + ".seq", codec);
		for (S3ObjectSummary summary : summaries) {
			String filename = summary.getKey();
			S3Object s3object = s3Client.getObject(summary.getBucketName(), summary.getKey());
			InputStream objectContent = s3object.getObjectContent();

			ext = "." + (ext.startsWith(".") ? ext.substring(1) : ext);

			if (filename.endsWith(".tar.gz")) {
				GZIPInputStream gzipInputStream = new GZIPInputStream(objectContent);
				TarArchiveInputStream tarArchiveInputStream = new TarArchiveInputStream(gzipInputStream);
				TarArchiveEntry tarArchiveEntry;
				while ((tarArchiveEntry = tarArchiveInputStream.getNextTarEntry()) != null) {
					if (!tarArchiveEntry.isDirectory()// not a dir and match ext
							&& tarArchiveEntry.getName().toLowerCase().contains(ext.toLowerCase())) {
						byte[] bytes = IOUtils.toByteArray(tarArchiveInputStream, tarArchiveEntry.getSize());
						logger.debug("tar filename={}", tarArchiveEntry.getName());
						String filenameInTar = StringUtils.substringBeforeLast(filename, "/")
								+ tarArchiveEntry.getName().replaceAll("^\\.", "");
						logger.debug("hdfs path={}", outpath + "/" + filenameInTar + ".seq");
						if (packingManyFilesToOneSequenceFile(writer, filenameInTar, bytes) == 0) {
							writer.close();
							writer = createSequenceFileWriter(outpath + "/" + seq++ + ".seq", codec);
						}
					}
				}
				tarArchiveInputStream.close();
			} else {
				if (filename.toLowerCase().contains(ext.toLowerCase())) {
					byte[] bytes;
					if (filename.endsWith(".bz2")) {
						objectContent.read();// read two bytes as a workround to
						objectContent.read();// to deal with bzip2 size issue
						bytes = IOUtils.toByteArray(new CBZip2InputStream(objectContent));
					} else if (filename.endsWith(".gz")) {
						bytes = IOUtils.toByteArray(new java.util.zip.GZIPInputStream(objectContent));
					} else if (filename.endsWith(".zip")) {
						bytes = IOUtils.toByteArray(new java.util.zip.ZipInputStream(objectContent));
					} else {// TODO other compression we care?
						if (filename.toLowerCase().endsWith(ext.toLowerCase()))
							bytes = IOUtils.toByteArray(objectContent);
						else
							continue; //skip if it is other meta data like *.<ext>.md5
					}
					logger.debug("hdfs path={}", outpath + "/" + filename + ".seq");
					if (packingManyFilesToOneSequenceFile(writer, filename, bytes) == 0) {
						writer.close();
						writer = createSequenceFileWriter(outpath + "/" + seq++ + ".seq", codec);
					}
				}
			}
			objectContent.close();
		}
	}

	/**
	 * Copy and pack files from AWS S3 as "balls" into a "box" with size=default HDFS, to maximize locality so each task
	 * node only fetches data locally at processing time.
	 * 
	 * @deprecated, 'coz Hadoop 2.3+ multiple name node
	 * 
	 * @param s3URI
	 * @param hdfsDir
	 * @param ext
	 *            case-insensitive filename extension, e.g., nc, or TIF,
	 * @param codec
	 * @throws IOException
	 */
	public static void packS3FilesToHDFS(String s3URI, String hdfsDir, String ext, CompressionCodec codec)
			throws IOException {
		conf.set("fs.defaultFS", getHadoopMasterURI());
		Path outpath;
		if (hdfsDir.startsWith("hdfs://"))
			outpath = new Path(hdfsDir.replaceAll("hdfs://[a-z\\.\\:0-9]+", ""));
		else
			outpath = new Path(hdfsDir); // TODO nn:port is required for now

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
		SequenceFile.Writer writer = createSequenceFileWriter(outpath + "/" + seq + ".seq", codec);
		for (S3ObjectSummary summary : summaries) {
			String filename = summary.getKey();
			logger.info("filename=" + filename);
			S3Object s3object = s3Client.getObject(summary.getBucketName(), summary.getKey());
			InputStream objectContent = s3object.getObjectContent();

			// remove leading . of extension if any and adding .
			ext = "." + (ext.startsWith(".") ? ext.substring(1) : ext);
			if (filename.endsWith(".tar.gz")) {
				GZIPInputStream gzipInputStream = new GZIPInputStream(objectContent);
				TarArchiveInputStream tarArchiveInputStream = new TarArchiveInputStream(gzipInputStream);
				TarArchiveEntry tarArchiveEntry;
				while ((tarArchiveEntry = tarArchiveInputStream.getNextTarEntry()) != null) {
					if (!tarArchiveEntry.isDirectory()// not a dir and match ext
							&& tarArchiveEntry.getName().toLowerCase().contains(ext.toLowerCase())) {
						byte[] bytes = IOUtils.toByteArray(tarArchiveInputStream, tarArchiveEntry.getSize());
						if (packingManyFilesToOneSequenceFile(writer, filename, bytes) == 0) {
							writer.close();
							writer = createSequenceFileWriter(outpath + "/" + seq++ + ".seq", codec);
						}
					}
				}
				tarArchiveInputStream.close();
			} else {
				if (filename.toLowerCase().contains(ext.toLowerCase())) {
					byte[] bytes;
					if (filename.endsWith(".bz2")) {
						objectContent.read();// read two bytes as a workround to
						objectContent.read();// to deal with bzip2 size issue
						bytes = IOUtils.toByteArray(new CBZip2InputStream(objectContent));
					} else if (filename.endsWith(".gz")) {
						bytes = IOUtils.toByteArray(new java.util.zip.GZIPInputStream(objectContent));
					} else if (filename.endsWith(".zip")) {
						bytes = IOUtils.toByteArray(new java.util.zip.ZipInputStream(objectContent));
					} else {// TODO other compression we care?
						bytes = IOUtils.toByteArray(objectContent);
					}
					if (packingManyFilesToOneSequenceFile(writer, filename, bytes) == 0) {
						writer.close();
						writer = createSequenceFileWriter(outpath + "/" + seq++ + ".seq", codec);
					}
				}
			}
			objectContent.close();
		}
		writer.close();
	}

	static Writer createSequenceFileWriter(String absolutepath, CompressionCodec codec) throws IOException {
		CompressionType compressionType;
		if (codec == null) {
			compressionType = CompressionType.NONE;
		} else {
			compressionType = CompressionType.BLOCK;
		}

		SequenceFile.Writer writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(new Path(absolutepath)),
				SequenceFile.Writer.compression(compressionType, codec), SequenceFile.Writer.keyClass(Text.class),
				SequenceFile.Writer.valueClass(BytesWritable.class));
		return writer;
	}

	//Deprecated
	static int packingManyFilesToOneSequenceFile(Writer sqwriter, String filename, byte[] fileinbytes)
			throws IOException {
		final double bufferZoneFactor = 0.9;// not to overflow HDFS block size
		Text key = new Text(filename);
		BytesWritable value = new BytesWritable(fileinbytes);
		// Since HDFS Block size is normally at 64~128MB range, int will be sufficient to hold the file size.
		// current position ~= actual sequencefile size
		sqwriter.append(key, value);
		int seqfilesize = (int) sqwriter.getLength();
		return (seqfilesize < (int) (getHDFSBlockSize() * bufferZoneFactor)) ? seqfilesize : 0;
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
					logger.debug("key={}, size={}" + objectSummary.getKey(), objectSummary.getSize());
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
		} else if (dir.startsWith("file://")) {
			String absolutePath = dir.replaceAll("^file://", "");
			Iterator<File> it = FileUtils.iterateFiles(new File(absolutePath), new String[] { ext }, Boolean.TRUE);
			while (it.hasNext()) {
				uri.add(it.next().getAbsolutePath());
			}
		} else {
			logger.error("{} source not supported", dir); // TODO: other sources
			new Exception("file source not supported");
		}
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

			String trimmedS3URI = inputURI.replaceAll("^s3[n]?:/{2,}", ""); //trim protocol part
			String bucket = StringUtils.substringBefore(trimmedS3URI, "/"); //the first as bucket
			String prefix = StringUtils.substringAfter(trimmedS3URI, "/"); //the rest are prefix

			//ObjectListing listing = s3Client.listObjects(bucket, prefix);
			/*
			 * String[] args = inputURI.split("/"); String bucket = args[2].split("\\.")[0];
			 * logger.debug("inputURI={},bucket={}", inputURI, bucket); List<String> argsList = new
			 * LinkedList<String>(Arrays.asList(args)); argsList.remove(0); argsList.remove(0); argsList.remove(0);//
			 * trimming leading protocol and bucket String object = StringUtils.join(argsList, "/");
			 */
			logger.debug("inputURI={}, bucket={}, prefix={}", inputURI, bucket, prefix);

			GetObjectRequest request = new GetObjectRequest(bucket, prefix);
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
		org.apache.hadoop.io.IOUtils.closeStream(writer);
	}

	public static void listSequenceFileKeys(String sequenceFileURI) throws Exception {
		Path path = null;
		if (sequenceFileURI.startsWith("hdfs://")) {
			if (!conf.get("fs.defaultFS").contains("hdfs://")) {
				conf.set("fs.defaultFS", getHadoopMasterURI());
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
		org.apache.hadoop.io.IOUtils.closeStream(reader);
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
			logger.info("key= {},value size={}", key.toString(), value.getBytes().length);
			return value.getBytes();
		}
		org.apache.hadoop.io.IOUtils.closeStream(reader);
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
			logger.debug("key={}, value size={}", key.toString(), value.getBytes().length);
			map.put(key, value.getBytes());
		}
		org.apache.hadoop.io.IOUtils.closeStream(reader);
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
