package com.openresearchinc.hadoop.sequencefile;

import static com.googlecode.javacv.cpp.opencv_core.cvClearMemStorage;
import static com.googlecode.javacv.cpp.opencv_core.cvGetSeqElem;
import static com.googlecode.javacv.cpp.opencv_core.cvLoad;
import static com.googlecode.javacv.cpp.opencv_objdetect.CV_HAAR_DO_CANNY_PRUNING;
import static com.googlecode.javacv.cpp.opencv_objdetect.cvHaarDetectObjects;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.imageio.ImageIO;
import javax.imageio.stream.ImageInputStream;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.javacv.cpp.opencv_core.CvMemStorage;
import com.googlecode.javacv.cpp.opencv_core.CvRect;
import com.googlecode.javacv.cpp.opencv_core.CvSeq;
import com.googlecode.javacv.cpp.opencv_core.IplImage;
import com.googlecode.javacv.cpp.opencv_objdetect.CvHaarClassifierCascade;

//@formatter:off
/**
 * 
 * Detect faces/eyes/.. from images encoded as Hadoop SequenceFile stored on hdfs:// or s3n://
 *  
 * JavaCV depedencies: 
 * 1. Download and unzip both javacv-0.7-bin.zip and javacv-0.7-cppjars.zip
 * 2. Specify additionalClasspathElement in pom.xml *   
 * otherwise there will be error like: 
 * java.lang.UnsatisfiedLinkError: no opencv_java248 in java.library.path
 * 
 * P.S. This needs to be done for each node(slave), e.g., if used in AWS EMR.
 * @author heq
 */
// @formatter:on
public class OpenCV {
	final static Logger logger = LoggerFactory.getLogger(OpenCV.class);
	static String hostname = "localhost";

	public static class Map extends MapReduceBase implements
			Mapper<Text, BytesWritable, Text, ArrayPrimitiveWritable> {

		public void map(Text key, BytesWritable value,
				OutputCollector<Text, ArrayPrimitiveWritable> output, Reporter reporter)
				throws IOException {
			Text outputkey = new Text();
			ArrayPrimitiveWritable outputvalue = new ArrayPrimitiveWritable();
			List<int[]> faces = new ArrayList<int[]>();
			String filename = key.toString();
			byte[] bytes = value.getBytes();
			if (filename.toLowerCase().matches(".*png.*|.*jpg.*|.*gif.*")) {
				BufferedImage rawimage = ImageIO.read(new ByteArrayInputStream(bytes));
				faces = detectFace(rawimage);
			} else if (filename.toLowerCase().matches(".*ppm.*")) {
				ImageInputStream iis = ImageIO.createImageInputStream(new ByteArrayInputStream(
						bytes));
				BufferedImage rawimage = PPMImageReader.read(iis);
				faces = detectFace(rawimage);
			} else {
				logger.error("unsupported image formats for input: " + filename);
				System.exit(1);
			}
			if (logger.isDebugEnabled())
				logger.debug("Detect " + faces.size() + " in " + filename + " on host " + hostname);

			for (int[] box : faces) {
				outputkey.set(filename);
				outputvalue.set(box);
				output.collect(outputkey, outputvalue);
			}
		}
	}

	/**
	 * A place holder reducer for further processing of detected face.
	 * 
	 * 
	 */
	public static class Reduce extends MapReduceBase implements
			Reducer<Text, ArrayPrimitiveWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<ArrayPrimitiveWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum++;
			}
			output.collect(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		String usage = "Usage: hadoop jar ./target/bin2seq*.jar com.openresearchinc.hadoop.sequencefile.OpenCV  <input-uri-images-on-hdfs-s3> <output-uri-of-detected-boxes-faces-in-each-image>";
		hostname = InetAddress.getLocalHost().getHostName();

		JobConf job = new JobConf(OpenCV.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		JobClient.runJob(job);
	}

	public static List<int[]> detectFace(BufferedImage rawimage) {
		String faceClassifierPath = new File(OpenCV.class.getResource(
				"/haarcascade_frontalface_alt.xml").getFile()).getAbsolutePath();
		return detect(rawimage, faceClassifierPath);
	}

	public static List<int[]> detectEye(BufferedImage rawimage) {
		String eyeClassifierPath = new File(OpenCV.class.getResource("/haarcascade_eye.xml")
				.getFile()).getAbsolutePath();
		return detect(rawimage, eyeClassifierPath);
	}

	static List<int[]> detect(BufferedImage rawimage, String classifierPath) {
		// store x-y of the box of detected face
		List<int[]> facebox = new ArrayList<int[]>();

		CvHaarClassifierCascade classifier = new CvHaarClassifierCascade(cvLoad(classifierPath));
		IplImage origImg = IplImage.createFrom(rawimage);
		CvMemStorage storage = CvMemStorage.create();
		CvSeq target = cvHaarDetectObjects(origImg, classifier, storage, 1.1, 3,
				CV_HAAR_DO_CANNY_PRUNING);
		cvClearMemStorage(storage);

		for (int i = 0; i < target.total(); i++) {
			CvRect r = new CvRect(cvGetSeqElem(target, i));
			facebox.add(new int[] { r.x(), r.y(), r.x() + r.width(), r.y() + r.height() });
			if (logger.isDebugEnabled())
				logger.debug("x=" + r.x() + " y=" + r.y() + " x+w=" + r.x() + r.width() + " y+h="
						+ r.y() + r.height());
		}
		if (logger.isDebugEnabled())
			logger.debug("Num of detected objects=" + target.total());
		return facebox;
	}

}
