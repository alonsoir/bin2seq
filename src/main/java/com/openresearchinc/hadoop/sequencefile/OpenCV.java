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
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.imageio.ImageIO;
import javax.imageio.stream.ImageInputStream;

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
public class OpenCV extends Configured implements Tool {
	final static Logger logger = LoggerFactory.getLogger(OpenCV.class);
	static String hostname = "localhost";
	static CvHaarClassifierCascade faceClassifier;
	static CvHaarClassifierCascade eyeClassifier;

	public static void main(String[] args) throws Exception {
		String usage = "Usage: hadoop jar ./target/bin2seq*.jar com.openresearchinc.hadoop.sequencefile.OpenCV  "
				+ "-libs $LIBJARS <input-uri-images-on-hdfs-s3> <output-uri-of-detected-boxes-faces-in-each-image>";

		int res = ToolRunner.run(new Configuration(), new OpenCV(), args);
		System.exit(res);

	}

	@Override
	public final int run(final String[] args) throws Exception {
		Job job = Job.getInstance(super.getConf());
		job.setJarByClass(OpenCV.class);

		job.setOutputKeyClass(Text.class); // filename
		// array of box containing face/eye/..
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		// job.setReducerClass(Reduce.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.addCacheFile(new URI(
				"s3n://ori-haarcascade/haarcascade_frontalface_default.xml#haarcascade_frontalface_default.xml"));
		job.addCacheFile(new URI("s3n://ori-haarcascade/haarcascade_eye.xml#haarcascade_eye.xml"));
		job.waitForCompletion(true);
		return 0;

	}

	public static class Map extends Mapper<Text, BytesWritable, Text, Text> {

		// cache classifiers when M/R starts
		protected void setup(Context context) throws IOException {
			// log where mapper is executed
			hostname = InetAddress.getLocalHost().getHostName();

			URI[] fileURIs = context.getCacheFiles();
			for (URI uri : fileURIs) {
				String localPath = uri.getPath();
				if (localPath.contains("haarcascade_frontalface_default.xml")) {
					faceClassifier = new CvHaarClassifierCascade(cvLoad(new File(
							"./haarcascade_frontalface_default.xml").getAbsolutePath()));
				} else if (localPath.contains("haarcascade_eye.xml")) {
					eyeClassifier = new CvHaarClassifierCascade(
							cvLoad(new File("./haarcascade_eye.xml").getAbsolutePath()));
				} else {
					// TODO
				}
			}
			if (faceClassifier == null && eyeClassifier == null) {
				logger.error("no detect classifier is defined");
				System.exit(1);
			}
		}

		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
			String filename = key.toString();
			Text outputkey = new Text(key);
			Text outputvalue = new Text();
			List<int[]> faces = new ArrayList<int[]>();
			byte[] bytes = value.getBytes();

			if (filename.toLowerCase().matches(".*png.*|.*jpg.*|.*gif.*")) {
				BufferedImage rawimage = ImageIO.read(new ByteArrayInputStream(bytes));
				faces = detectFace(rawimage);
			} else if (filename.toLowerCase().matches(".*ppm.*")) {
				ImageInputStream iis = ImageIO.createImageInputStream(new ByteArrayInputStream(bytes));
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
				outputvalue.set(Arrays.toString(box));
				context.write(outputkey, outputvalue);
			}
		}
	}

	// detect face from image and return box coordinates for detected face
	public static List<int[]> detectFace(BufferedImage rawimage) {
		// if invoke by M/R, pick up faceClassifier from distributed cache
		// if invoke by test class, pick up from source tree
		if (faceClassifier == null)
			faceClassifier = new CvHaarClassifierCascade(cvLoad(new File(OpenCV.class.getResource(
					"/haarcascade_frontalface_default.xml").getFile()).getAbsolutePath()));
		return detect(rawimage, faceClassifier);
	}

	// detect face from image and return box coordinates for detected eyes
	public static List<int[]> detectEye(BufferedImage rawimage) {
		// if invoke by M/R, pick up faceClassifier from distributed cache
		// if invoke by test class, pick up from source tree
		if (eyeClassifier == null)
			eyeClassifier = new CvHaarClassifierCascade(cvLoad(new File(OpenCV.class
					.getResource("/haarcascade_eye.xml").getFile()).getAbsolutePath()));
		return detect(rawimage, eyeClassifier);
	}

	static List<int[]> detect(BufferedImage rawimage, CvHaarClassifierCascade classifier) {
		// store x-y of the box of detected face
		List<int[]> facebox = new ArrayList<int[]>();
		IplImage origImg = IplImage.createFrom(rawimage);
		CvMemStorage storage = CvMemStorage.create();
		CvSeq target = cvHaarDetectObjects(origImg, classifier, storage, 1.1, 3, CV_HAAR_DO_CANNY_PRUNING);
		cvClearMemStorage(storage);

		for (int i = 0; i < target.total(); i++) {
			CvRect r = new CvRect(cvGetSeqElem(target, i));
			facebox.add(new int[] { r.x(), r.y(), r.x() + r.width(), r.y() + r.height() });
			if (logger.isDebugEnabled())
				logger.debug("x=" + r.x() + " y=" + r.y() + " x+w=" + r.x() + r.width() + " y+h=" + r.y() + r.height());
		}
		if (logger.isDebugEnabled())
			logger.debug("Num of detected objects=" + target.total());
		return facebox;
	}

}
