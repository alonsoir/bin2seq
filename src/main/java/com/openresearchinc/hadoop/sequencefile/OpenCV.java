package com.openresearchinc.hadoop.sequencefile;

import static com.googlecode.javacv.cpp.opencv_core.cvClearMemStorage;
import static com.googlecode.javacv.cpp.opencv_core.cvLoad;
import static com.googlecode.javacv.cpp.opencv_objdetect.CV_HAAR_DO_CANNY_PRUNING;
import static com.googlecode.javacv.cpp.opencv_objdetect.cvHaarDetectObjects;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.javacpp.Loader;
import com.googlecode.javacv.cpp.opencv_core.CvMemStorage;
import com.googlecode.javacv.cpp.opencv_core.CvSeq;
import com.googlecode.javacv.cpp.opencv_core.IplImage;
import com.googlecode.javacv.cpp.opencv_objdetect;
import com.googlecode.javacv.cpp.opencv_objdetect.CvHaarClassifierCascade;

//import javax.imageio.ImageIO;

/**
 * cp <opencv-path>/release/lib/libopencv_java248.so <hadoop-path>/lib/native/.
 * otherwise will get following problem java.lang.UnsatisfiedLinkError: no
 * opencv_java248 in java.library.path
 * 
 * @author heq
 * 
 */
public class OpenCV {
	private static final Logger logger = LoggerFactory.getLogger(OpenCV.class);
	private static final Configuration conf = new Configuration();

	public static void main(String[] args) throws Exception {
		String usage = "Usage: hadoop jar ./target/*.jar com.openresearchinc.hadoop.sequencefile.OpenCV -in <input-uri-image> ";
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		List<String> argList = Arrays.asList(otherArgs);
		int pos = argList.indexOf("-in");
		if (pos == -1) {
			System.err.println(usage);
			System.exit(2);
		}
		detectFace(otherArgs[pos + 1]);
	}

	/**
	 * Detect face from image files in sequencefile format stored on a HDFS
	 * directory
	 * 
	 * @param hdfsdir
	 *            e.g., hdfs://master:8020/
	 * @param ext
	 *            e.g., seq, standard for image file in sequencefile format.
	 * @throws IOException
	 **/

	public static void detectFace(String hdfsdir, String ext) throws Exception {
		if (hdfsdir.contains("hdfs://")) {
			conf.set("fs.defaultFS", hdfsdir);
			FileStatus[] status = FileSystem.get(conf).listStatus(new Path(hdfsdir));
			for (int i = 0; i < status.length; i++) {
				Path path = status[i].getPath();
				if (path.getName().endsWith(ext) && status[i].getLen() > 0 && !status[i].isSymlink()) {
					logger.debug(status[i].getPath() + " " + status[i].getLen());
					detectFace(path.toString());
				}
			}
		} else if (hdfsdir.contains("s3://")) {
			List<String> sequris = Util.listFiles(hdfsdir, "seq");
			for (String uri : sequris) {
				detectFace(uri);
			}
		}
	}

	public static int detectFace(String uri) throws Exception {
		Map<Text, byte[]> imagesequnce = Util.readSequenceFile(uri);

		for (Map.Entry<Text, byte[]> entry : imagesequnce.entrySet()) {
			String filename = entry.getKey().toString();
			BufferedImage rawimage = ImageIO.read(new ByteArrayInputStream(entry.getValue()));
			if (rawimage == null) { // not an image
				logger.warn("not an image");
				return 0;
			}

			// File file = Loader.extractResource(
			// OpenCV.class.getClassLoader().getResource("haarcascade_frontalface_alt.xml").toURI().toURL(),
			// null,
			// "classifier", ".xml");
			CvHaarClassifierCascade classifier = new CvHaarClassifierCascade(cvLoad("haarcascade_frontalface_alt.xml"));
			IplImage origImg = IplImage.createFrom(rawimage);
			CvMemStorage storage = CvMemStorage.create();
			CvSeq faces = cvHaarDetectObjects(origImg, classifier, storage, 1.1, 3, CV_HAAR_DO_CANNY_PRUNING);
			cvClearMemStorage(storage);
			logger.info("file=" + filename + ": # faces=" + faces.total());
			return faces.total();
		}
		return 0;
	}
}
