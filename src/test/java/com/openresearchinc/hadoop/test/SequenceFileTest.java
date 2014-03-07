package com.openresearchinc.hadoop.test;

//Credit to blog:http://noushinb.blogspot.com/2013/04/reading-writing-hadoop-sequence-files.html

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Random;

import ncsa.hdf.object.h5.H5File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.log4j.Logger;
import org.junit.Test;

import ucar.ma2.Array;
import ucar.ma2.ArrayDouble;
import ucar.ma2.ArrayFloat;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.openresearchinc.hadoop.sequencefile.Util;
import com.openresearchinc.hadoop.sequencefile.hdf5_getters;

public class SequenceFileTest {
	private final static Logger logger = Logger.getLogger(Util.class);
	private final static ClientConfiguration config = new ClientConfiguration();// .withProxyHost("http_proxy").withProxyPort(80);
	private final static AmazonS3 s3 = new AmazonS3Client(
			new BasicAWSCredentials(System.getenv("AWS_ACCESS_KEY_ID"),
					System.getenv("AWS_SECRET_KEY")), config);

	@Test
	/**
	 *
	 * @throws Exception
	 */
	public void testCopyRecursivelyFromS3() throws Exception {
		String bucketName = "nasanex";
		String prefix = "NEX-DCP30/BCSD/rcp26/mon/atmos/pr/r1i1p1/v1.0/";
		String extension = "nc";

		ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
				.withBucketName(bucketName).withPrefix(prefix);
		ObjectListing objectListing;

		do {
			objectListing = s3.listObjects(listObjectsRequest);
			for (S3ObjectSummary objectSummary : objectListing
					.getObjectSummaries()) {
				if (!objectSummary.getKey().endsWith(extension))
					continue;
				logger.info(" - " + objectSummary.getKey() + "  " + "(size = "
						+ objectSummary.getSize() + ")");
				String inputURI = "s3://" + bucketName + ".s3.amazonaws.com/"
						+ prefix + objectSummary.getKey();
				Util.writeToSequenceFile(inputURI, "hdfs://master:8020/"
						+ prefix + objectSummary.getKey() + ".seq",
						new SnappyCodec());//TODO have not tested on Hadoop yet.  
			}
			listObjectsRequest.setMarker(objectListing.getNextMarker());
		} while (objectListing.isTruncated());
	}

	@Test
	/**
	 * Find min/max/average precipitation for a randomly-positioned but fixed-size region from a nc file
	 * output: filename,origin,size key: value:min, max, average  
	 * @throws Exception
	 */
	public void testProcessingNASANexData() throws Exception {
		final int SIZE = 100;
		File file = new File(this.getClass().getResource("/ncar.nc").getPath());
		byte[] netcdfinbyte = FileUtils.readFileToByteArray(file);
		NetcdfFile netCDFfile = NetcdfFile.openInMemory("inmemory.nc",
				netcdfinbyte);// use any dummy filename for file in memory

		Variable time = netCDFfile.findVariable("time");
		ArrayDouble.D1 days = (ArrayDouble.D1) time.read();
		Variable lat = netCDFfile.findVariable("lat");
		if (lat == null) {
			logger.error("Cannot find Variable latitude(lat)");
			return;
		}
		ArrayFloat.D1 absolutelat = (ArrayFloat.D1) lat.read();
		Variable lon = netCDFfile.findVariable("lon");
		if (lon == null) {
			logger.error("Cannot find Variable longitude(lon)");
			return;
		}
		ArrayFloat.D1 absolutelon = (ArrayFloat.D1) lon.read();
		Variable pres = netCDFfile.findVariable("pr");
		if (pres == null) {
			logger.error("Cannot find Variable precipitation(pr)");
			return;
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
		logger.info(days + "," + absolutelat.get(orig_lat) + ","
				+ absolutelon.get(orig_lon) + "," + SIZE + ":" + min + ","
				+ max + "," + sum / (SIZE * SIZE));
	}

	@Test
	// Test support (indirect) open HDF5 file from memory using netcdf API
	// TODO: python
	// http://stackoverflow.com/questions/16654251/can-h5py-load-a-file-from-a-byte-array-in-memory
	public void testNetcdfToH5() throws Exception {
		H5File h5 = hdf5_getters.hdf5_open_readonly(this.getClass()
				.getResource("/TRAXLZU12903D05F94.h5").getPath());
		double h5_temp = hdf5_getters.get_tempo(h5);

		File file = new File(this.getClass()
				.getResource("/TRAXLZU12903D05F94.h5").getPath());
		byte[] netcdfinbyte = FileUtils.readFileToByteArray(file);
		NetcdfFile netCDFfile = NetcdfFile.openInMemory("inmemory.h5",
				netcdfinbyte);// use any dummy filename for file in memory
		Variable var = (Variable) netCDFfile
				.findVariable("/analysis/songs.tempo");
		Array content = var.read();// 1D array
		double netcdf_tempo = content.getDouble(0); // 1 column only
		assertEquals(h5_temp, netcdf_tempo, 0.001);
	}

	@Test
	public void testReadnetCDFinSequnceFileFormat() throws IOException {

		String path = this.getClass().getResource("/ncar.nc").getPath();
		Util.writeToSequenceFile("file://" + path,
				"hdfs://master:8020/tmp/ncar.seq", new DefaultCodec());
		Map<Text, byte[]> netcdfsequnce = Util
				.readSequenceFile("hdfs://master:8020/tmp/ncar.seq");
		for (Map.Entry<Text, byte[]> entry : netcdfsequnce.entrySet()) {
			NetcdfFile ncFile = NetcdfFile.openInMemory(entry.getKey()
					.toString(), entry.getValue());
			assertEquals(ncFile.getDimensions().size(), 5);
		}
	}

	@Test
	public void testDataAdaptors() throws Exception {
		Util.writeToSequenceFile("file:///etc/passwd",
				"file:///tmp/passwd.seq", new DefaultCodec());
		Map<Text, byte[]> passwd = Util
				.readSequenceFile("file:///tmp/passwd.seq");
		for (Map.Entry<Text, byte[]> entry : passwd.entrySet()) {
			assertEquals(entry.getKey().toString(), "/etc/passwd");
		}
	}

	@Test
	public void testConfig() {
		Configuration conf = new Configuration();
		Util.listHadoopConfiguration(conf);
	}

	@Test
	//@formatter:off
	// get Hadoop source from http://apache.mirrors.tds.net/hadoop/common/stable/hadoop-2.2.0-src.tar.gz
	// cd hadoop-common-project/hadoop-common
	// $mvn compile -Pnative
	// cp hadoop-common/target/native/target/usr/local/lib/libhadoop.so ~/hadoop-2.2.0/lib/native/.
	// library -Djava.library.path=/home/heq/hadoop-2.2.0/lib/native
	//@formatter:on
	public void testCodecs() throws Exception {
		String path = this.getClass().getResource("/ncar.nc").getPath();
		Util.writeToSequenceFile("file://" + path,
				"hdfs://master:8020/tmp/ncar.seq", new GzipCodec());
		Util.writeToSequenceFile("file://" + path,
				"hdfs://master:8020/tmp/ncar.seq", new BZip2Codec());
		Util.writeToSequenceFile("file://" + path,
				"hdfs://master:8020/tmp/ncar.seq", new Lz4Codec());
		Util.writeToSequenceFile("file://" + path,
				"hdfs://master:8020/tmp/ncar.seq", new SnappyCodec());
	}

	@Test
	public void testListSequenceFile() throws Exception {
		Util.listSequenceFileKeys("hdfs://master:8020/tmp/ncar.seq");
	}

	@Test
	public void testS3() throws Exception {

		String inputURI = "http://nasanex.s3.amazonaws.com/NEX-DCP30/BCSD/rcp26/mon/atmos/pr/r1i1p1/v1.0/CONUS/pr_amon_BCSD_rcp26_r1i1p1_CONUS_HadGEM2-ES_200512-200512.nc";
		Util.writeToSequenceFile(inputURI,
				"hdfs://master:8020/tmp/nasa-nc.seq", new GzipCodec());

		String existingBucketName = "ori-tmp"; // dir
		String keyName = "passwd"; // file
		inputURI = "s3://" + existingBucketName + ".s3.amazonaws.com/"
				+ keyName;
		Util.writeToSequenceFile(inputURI, "file:///tmp/passwd.seq",
				new GzipCodec());
		Util.writeToSequenceFile(inputURI, "hdfs://master:8020/tmp/passwd.seq",
				new SnappyCodec());
	}
}
