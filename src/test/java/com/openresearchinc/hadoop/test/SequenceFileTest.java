package com.openresearchinc.hadoop.test;

//Credit to blog:http://noushinb.blogspot.com/2013/04/reading-writing-hadoop-sequence-files.html

import static com.googlecode.javacv.cpp.opencv_core.cvClearMemStorage;
import static com.googlecode.javacv.cpp.opencv_core.cvLoad;
import static com.googlecode.javacv.cpp.opencv_objdetect.CV_HAAR_DO_CANNY_PRUNING;
import static com.googlecode.javacv.cpp.opencv_objdetect.cvHaarDetectObjects;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.imageio.ImageIO;

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

import com.googlecode.javacpp.Loader;
import com.googlecode.javacv.cpp.opencv_core.CvMemStorage;
import com.googlecode.javacv.cpp.opencv_core.CvSeq;
import com.googlecode.javacv.cpp.opencv_core.IplImage;
import com.googlecode.javacv.cpp.opencv_objdetect;
import com.googlecode.javacv.cpp.opencv_objdetect.CvHaarClassifierCascade;
import com.openresearchinc.hadoop.sequencefile.OpenCV;
import com.openresearchinc.hadoop.sequencefile.Util;
import com.openresearchinc.hadoop.sequencefile.hdf5_getters;

public class SequenceFileTest {
	private final static Logger logger = Logger.getLogger(Util.class);
	private final static String hadoopMaster = "master:8020";

	@Test
	public void testjavacv() throws Exception {
		String faceClassifierPath = new File(this.getClass().getResource("/haarcascade_frontalface_alt.xml").getFile())
				.getAbsolutePath();
		BufferedImage rawimage = ImageIO.read(new File(this.getClass().getResource("/lena.png").getFile()));
		IplImage origImg = IplImage.createFrom(rawimage);
		CvMemStorage storage = CvMemStorage.create();
		CvHaarClassifierCascade classifier = new CvHaarClassifierCascade(cvLoad(faceClassifierPath));
		cvClearMemStorage(storage);
		CvSeq faces = cvHaarDetectObjects(origImg, classifier, storage, 1.1, 3, CV_HAAR_DO_CANNY_PRUNING);
		// Loop the number of faces found. Draw red box around face.
		int total = faces.total();
		logger.info("total=" + total);
	}

	@Test
	/**
	 * Encode a lena image into sequencefile, read from hdfs into memory, detect with opencv
	 * 
	 * -Djava.library.path=/home/heq/opencv/release/lib:/home/heq/hadoop-2.2.0/lib/native 
	 * @throws Exception
	 */
	public void testProcessingImageInSequenceFile() throws Exception {
		// System.loadLibrary(Core.NATIVE_LIBRARY_NAME);

		String imagePath = new File(this.getClass().getResource("/lena.png").getFile()).getAbsolutePath();
		String inputURI = "file://" + imagePath;
		String outputURI = "hdfs://" + hadoopMaster + "/tmp/lena.png.seq";

		Util.writeToSequenceFile(inputURI, outputURI, new SnappyCodec());
		// detect a single file, make it more test-able
		// int faces = OpenCV.detectFace("s3://ori-tmp/lena.png.seq");
		// assertTrue(faces >= 1);

		int faces = OpenCV.detectFace(outputURI);
		assertTrue(faces >= 1);
		// detect all image files under hdfs dirs
		// OpenCV.detectFace("hdfs://" + hadoopMaster + "/tmp", "seq");

		// hadoop distcp hdfs://master:8020/tmp/lena.png.seq
		// s3://ori-tmp/lena.png.seq
		// faces = OpenCV.detectFace("s3://ori-tmp/lena.png.seq");
		// assertTrue(faces >= 1);
	}

	@Test
	/**
	 *  
	 * @throws Exception
	 */
	public void testLocalOpenCVFaceDetection() throws Exception {
		// Note: in eclipse, add user library (opencv),
		// external build/opencv-248.jar to opencv user lib,
		// point native library location to build/java/x86 or x64

		// String[] haar_configs = { "/haarcascade_frontalface_alt.xml",\
		// "/haarcascade_eye.xml" }; //TODO
		String[] haar_configs = { "/haarcascade_frontalface_alt.xml" };
		for (String haar : haar_configs) {
			File classifierFile = new File(OpenCV.class.getResource(haar).toURI());
			Loader.load(opencv_objdetect.class);
			CvHaarClassifierCascade classifier = new CvHaarClassifierCascade(cvLoad(classifierFile.getAbsolutePath()));
			IplImage origImg = IplImage.createFrom(ImageIO.read(new File(this.getClass().getResource("/lena.png")
					.getFile())));
			CvMemStorage storage = CvMemStorage.create();
			CvSeq faces = cvHaarDetectObjects(origImg, classifier, storage, 1.1, 3, CV_HAAR_DO_CANNY_PRUNING);
			cvClearMemStorage(storage);
			assertTrue(faces.total() >= 1);
		}
	}

	@Test
	/**
	 * List NASA OpenNex netCDF files under an randomly-selected folder
	 * @throws Exception
	 */
	public void testListFilesRecursivelyFromS3() throws Exception {
		List<String> ncfiles = Util.listFiles("s3://nasanex/NEX-DCP30/BCSD/rcp26/mon/atmos/pr/r1i1p1/v1.0/", "nc");
		assertTrue(ncfiles.size() >= 100); // a lot
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
	// Test support (indirect) open HDF5 file from memory using netcdf API
	// TODO: python
	// http://stackoverflow.com/questions/16654251/can-h5py-load-a-file-from-a-byte-array-in-memory
	public void testNetcdfToH5() throws Exception {
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
	public void testReadnetCDFinSequnceFileFormat() throws IOException {

		String path = this.getClass().getResource("/ncar.nc").getPath();
		Util.writeToSequenceFile("file://" + path, "hdfs://" + hadoopMaster + "/tmp/ncar.seq", new DefaultCodec());
		Map<Text, byte[]> netcdfsequnce = Util.readSequenceFile("hdfs://" + hadoopMaster + "/tmp/ncar.seq");
		for (Map.Entry<Text, byte[]> entry : netcdfsequnce.entrySet()) {
			NetcdfFile ncFile = NetcdfFile.openInMemory(entry.getKey().toString(), entry.getValue());
			assertEquals(ncFile.getDimensions().size(), 5);
		}
	}

	@Test
	public void testDataAdaptors() throws Exception {
		Util.writeToSequenceFile("file:///etc/passwd", "file:///tmp/passwd.seq", new DefaultCodec());
		Map<Text, byte[]> passwd = Util.readSequenceFile("file:///tmp/passwd.seq");
		for (Map.Entry<Text, byte[]> entry : passwd.entrySet()) {
			assertEquals(entry.getKey().toString(), "/etc/passwd");
		}
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
		Util.writeToSequenceFile("file://" + path, "hdfs://" + hadoopMaster + "/tmp/ncar.seq", new GzipCodec());
		Util.writeToSequenceFile("file://" + path, "hdfs://" + hadoopMaster + "/tmp/ncar.seq", new BZip2Codec());
		Util.writeToSequenceFile("file://" + path, "hdfs://" + hadoopMaster + "/tmp/ncar.seq", new Lz4Codec());
		Util.writeToSequenceFile("file://" + path, "hdfs://" + hadoopMaster + "/tmp/ncar.seq", new SnappyCodec());
	}

	@Test
	public void testListSequenceFile() throws Exception {
		Util.writeToSequenceFile("file:///etc/passwd", "file:///tmp/passwd.seq", new DefaultCodec());
		Util.listSequenceFileKeys("hdfs://" + hadoopMaster + "/tmp/passwd.seq");
	}

	@Test
	public void testS3() throws Exception {

		String inputURI = "http://nasanex.s3.amazonaws.com/NEX-DCP30/BCSD/rcp26/mon/atmos/pr/r1i1p1/v1.0/CONUS/pr_amon_BCSD_rcp26_r1i1p1_CONUS_HadGEM2-ES_200512-200512.nc";
		Util.writeToSequenceFile(inputURI, "hdfs://" + hadoopMaster + "/tmp/nasa-nc.seq", new GzipCodec());

		String existingBucketName = "ori-tmp"; // dir
		String keyName = "passwd"; // file
		inputURI = "s3://" + existingBucketName + ".s3.amazonaws.com/" + keyName;
		Util.writeToSequenceFile(inputURI, "file:///tmp/passwd.seq", new GzipCodec());
		Util.writeToSequenceFile(inputURI, "hdfs://" + hadoopMaster + "/tmp/passwd.seq", new SnappyCodec());
	}

	@Test
	public void testBatchEncoding() throws Exception {
		List<String> ncfiles = Util.listFiles(
				"s3://nasanex/MODIS/MOLT/MOD13Q1.005/2013.09.30/MOD13Q1.A2013273.h21v00.005.2013303115726.hdf", "hdf");
		for (String uri : ncfiles) {
			String output = new File(uri).getName();
			Util.writeToSequenceFile(uri, "hdfs://" + hadoopMaster + "/tmp/" + output + ".seq", new DefaultCodec());
		}
	}
}
