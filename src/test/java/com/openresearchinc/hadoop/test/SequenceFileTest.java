package com.openresearchinc.hadoop.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Random;

import ncsa.hdf.object.h5.H5File;

import org.apache.commons.io.FileUtils;
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

import com.openresearchinc.hadoop.sequencefile.OpenCV;
import com.openresearchinc.hadoop.sequencefile.Util;
import com.openresearchinc.hadoop.sequencefile.hdf5_getters;

/**
 * mvn test -Dtest=SequenceFileTest#<method>
 * 
 * @author Qiming He
 * 
 */
public class SequenceFileTest extends BaseTest {
	final static Logger logger = Logger.getLogger(SequenceFileTest.class);

	@Test
	public void testTmp() throws Exception {

		Map<Text, byte[]> ppm = Util.readSequenceFileFromS3("s3n://ori-hadoop/tmp/00001_930831_fa_a.ppm.seq");
		Map<Text, byte[]> ncar = Util.readSequenceFileFromHDFS(hadoopMaster + "/tmp/ncar.seq");

	}

	@Test
	/**
	 *  Test image in compressed PPM format as used in NIST Colorferet database 
	 *  Eclipse: -Djava.library.path=/home/heq/hadoop-2.2.0/lib/native
	 * @throws Exception
	 */
	public void testFaceDetectionInPPMFromS3() throws Exception {
		String file = "00001_930831_hl_a.ppm";
		String inputURI = "s3://ori-colorferetsubset/00001/" + file + ".bz2";
		String outputURI = hadoopMaster + "/tmp/" + file + ".seq";
		Util.writeToSequenceFile(inputURI, outputURI, new SnappyCodec());
		int faces = OpenCV.detectFaceInPPM(outputURI);
		assertTrue(faces == 1);
	}

	@Test
	/**
	 * 1.	JaveCV Face Detect image in SequenceFile from S3://
	 * 2.  	JaveCV Face Detect image in SequenceFile from hdfs://
	 * Before hdfs over HDFS is implemented, do $hadoop fs -cp  hdfs://<path>/tmp/lena.png.seq s3://ori-tmp/lena.png.seq
	 * Eclipse: -Djava.library.path=/home/heq/hadoop-2.2.0/lib/native 
	 * @throws Exception
	 */
	public void testJavaCVFaceDetectionFromS3HDFS() throws Exception {
		String inputURI = "file://" + new File(this.getClass().getResource("/lena.png").getFile()).getAbsolutePath();
		String s3URI = "s3n://ori-tmp/lena.png.seq";
		Util.writeToSequenceFile(inputURI, s3URI, new SnappyCodec());
		int faces = OpenCV.detectFaceinPngJpgEtc(s3URI);
		assertTrue(faces == 1);

		String hdfsURI = "s3n://ori-tmp/tmp/lena.png.seq";
		Util.writeToSequenceFile(inputURI, hdfsURI, new SnappyCodec());
		faces = OpenCV.detectFaceinPngJpgEtc(hdfsURI);
		assertTrue(faces == 1);
	}

	@Test
	public void testBatchFaceDetectionFromHDFS() throws Exception {
		OpenCV.detectFacesInDir(hadoopMaster + "/tmp/", "ppm.seq");
	}

	@Test
	/**
	 * List NASA OpenNex netCDF files under an randomly-selected folder
	 * @throws Exception
	 */
	public void testCopyFilesRecursivelyFromS3() throws Exception {
		List<String> ncfiles = Util.listFiles("s3://nasanex/NEX-DCP30/BCSD/rcp26/mon/atmos/pr/r1i1p1/v1.0/", "nc");
		assertTrue(ncfiles.size() >= 100); // a lot

		List<String> fileUrls = Util.listFiles("s3://ori-colorferetsubset/00001", "bz2");
		for (String url : fileUrls) {
			logger.info(url);
			String file = org.apache.commons.io.FilenameUtils.getBaseName(url);
			Util.writeToSequenceFile(url, hadoopMaster + "/tmp/" + file + ".seq", new SnappyCodec());
		}
	}

	@Test
	/**
	 * Find min/max/average precipitation for a randomly-positioned but fixed-size region from a nc file
	 * output: filename,origin,size key: value:min, max, average  
	 * @throws Exception
	 */
	public void testProcessingNASANexDataInNetCDF() throws Exception {
		final int SIZE = 100;
		File file = new File(this.getClass().getResource("/ncar.nc").getPath());
		byte[] netcdfinbyte = FileUtils.readFileToByteArray(file);
		// use any dummy filename for file in memory
		NetcdfFile netCDFfile = NetcdfFile.openInMemory("inmemory.nc", netcdfinbyte);

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
		logger.info(days + "," + absolutelat.get(orig_lat) + "," + absolutelon.get(orig_lon) + "," + SIZE + ":" + min
				+ "," + max + "," + sum / (SIZE * SIZE));
	}

	@Test
	/**
	 * TODO: python API: http://stackoverflow.com/questions/16654251/can-h5py-load-a-file-from-a-byte-array-in-memory
	 * @throws Exception
	 */
	public void testNetCDFInterfaceToACcessH5() throws Exception {
		logger.info("java.library.path=" + System.getProperty("java.library.path"));
		H5File h5 = hdf5_getters.hdf5_open_readonly(this.getClass().getResource("/TRAXLZU12903D05F94.h5").getPath());
		double h5_temp = hdf5_getters.get_tempo(h5);

		File file = new File(this.getClass().getResource("/TRAXLZU12903D05F94.h5").getPath());
		byte[] netcdfinbyte = FileUtils.readFileToByteArray(file);

		NetcdfFile netCDFfile = NetcdfFile.openInMemory("inmemory.h5", netcdfinbyte);
		Variable var = (Variable) netCDFfile.findVariable("/analysis/songs.tempo");
		Array content = var.read();// 1D array
		double netcdf_tempo = content.getDouble(0); // 1 column only
		assertEquals(h5_temp, netcdf_tempo, 0.001);
	}

	@Test
	public void testReadnetCDFinSequnceFileFormat() throws Exception {

		String path = this.getClass().getResource("/ncar.nc").getPath();
		Util.writeToSequenceFile("file://" + path, hadoopMaster + "/tmp/ncar.seq", new DefaultCodec());
		Map<Text, byte[]> netcdfsequnce = Util.readSequenceFile(hadoopMaster + "/tmp/ncar.seq");
		for (Map.Entry<Text, byte[]> entry : netcdfsequnce.entrySet()) {
			NetcdfFile ncFile = NetcdfFile.openInMemory(entry.getKey().toString(), entry.getValue());
			assertEquals(ncFile.getDimensions().size(), 5);
		}
	}

	@Test
	public void testReadWriteFromNativeFSToHDFS() throws Exception {
		Util.writeToSequenceFile("file:///etc/passwd", "file:///tmp/passwd.seq", new DefaultCodec());
		Map<Text, byte[]> passwd = Util.readSequenceFile("file:///tmp/passwd.seq");
		for (Map.Entry<Text, byte[]> entry : passwd.entrySet()) {
			assertEquals(entry.getKey().toString(), "/etc/passwd");
		}
	}

	@Test
	public void testGzipBzip2Lz4SnappyCodecs() throws Exception {
		String path = this.getClass().getResource("/ncar.nc").getPath();
		Util.writeToSequenceFile("file://" + path, hadoopMaster + "/tmp/ncar.seq", new GzipCodec());
		Util.writeToSequenceFile("file://" + path, hadoopMaster + "/tmp/ncar.seq", new BZip2Codec());
		Util.writeToSequenceFile("file://" + path, hadoopMaster + "/tmp/ncar.seq", new Lz4Codec());
		Util.writeToSequenceFile("file://" + path, hadoopMaster + "/tmp/ncar.seq", new SnappyCodec());
	}

	@Test
	public void testListSequenceFileKey() throws Exception {
		Util.writeToSequenceFile("file:///etc/passwd", "file:///tmp/passwd.seq", new DefaultCodec());
		Util.listSequenceFileKeys(hadoopMaster + "/tmp/passwd.seq");
	}

	@Test
	public void testCopyfromS3ViaHttpToHdfs() throws Exception {
		String inputURI = "http://nasanex.s3.amazonaws.com/NEX-DCP30/BCSD/rcp26/mon/atmos/pr/r1i1p1/v1.0/CONUS/pr_amon_BCSD_rcp26_r1i1p1_CONUS_HadGEM2-ES_200512-200512.nc";
		Util.writeToSequenceFile(inputURI, hadoopMaster + "/tmp/nasa-nc.seq", new SnappyCodec());

		String existingBucketName = "ori-tmp"; // dir
		String keyName = "passwd"; // file
		inputURI = "s3://" + existingBucketName + ".s3.amazonaws.com/" + keyName;
		Util.writeToSequenceFile(inputURI, "file:///tmp/passwd.seq", new SnappyCodec());
		Util.writeToSequenceFile(inputURI, hadoopMaster + "/tmp/passwd.seq", new SnappyCodec());
	}

	@Test
	public void testRecursiveCopyAndEncodingFromS3ToHdfs() throws Exception {
		List<String> ncfiles = Util.listFiles(
				"s3://nasanex/MODIS/MOLT/MOD13Q1.005/2013.09.30/MOD13Q1.A2013273.h21v00.005.2013303115726.hdf", "hdf");
		for (String uri : ncfiles) {
			String output = new File(uri).getName();
			Util.writeToSequenceFile(uri, hadoopMaster + "/tmp/" + output + ".seq", new DefaultCodec());
		}
	}
}
