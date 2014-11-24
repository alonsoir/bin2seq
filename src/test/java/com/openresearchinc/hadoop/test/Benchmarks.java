package com.openresearchinc.hadoop.test;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.util.List;

import javax.imageio.ImageIO;
import javax.imageio.stream.ImageInputStream;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.junit.Test;

import com.openresearchinc.hadoop.sequencefile.OpenCV;
import com.openresearchinc.hadoop.sequencefile.PPMImageReader;
import com.openresearchinc.hadoop.sequencefile.Util;

public class Benchmarks extends BaseTest {
	final static Logger logger = Logger.getLogger(Benchmarks.class);

	// @formatter:off
	/**
	 * Prepare colorferet dataset in sequencefile format in both s3:// and
	 * hdfs://
	 * 
	 * @throws Exception
	 */
	// @formatter:on

	@Test
	// mvn test -Dtest=Benchmarks#Colorferet
	public void Colorferet() throws Exception {
		String dataRoot = "s3://ori-colorferet-untar/";
		List<String> ppmbz2files = Util.listFiles(dataRoot, "ppm.bz2");
		logger.info("total files=" + ppmbz2files.size());
		DateTime startwatch = new DateTime();
		for (String ppmbz2 : ppmbz2files) {
			String basename = FilenameUtils.getBaseName(ppmbz2);
			Util.writeToSequenceFile(ppmbz2, hadoopMaster + "/colorferet/"
					+ basename + ".seq", new SnappyCodec());
		}
		DateTime stopwatch = new DateTime();
		logger.info("Time(ms) to copy Colorferet data from S3 (*.ppm.bz2) to HDFS (*.seq)="
				+ (stopwatch.getMillis() - startwatch.getMillis()));

		startwatch = new DateTime();
		for (String ppmbz2 : ppmbz2files) {
			String basename = FilenameUtils.getBaseName(ppmbz2);
			Util.writeToSequenceFile(ppmbz2, "s3n://ori-colorferet-seq/"
					+ basename + ".seq", new SnappyCodec());
		}
		stopwatch = new DateTime();
		logger.info("Time(ms) to copy Colorferet data from S3 (*.ppm.bz2) to S3 (*.seq)="
				+ (stopwatch.getMillis() - startwatch.getMillis()));

		startwatch = new DateTime();
		for (String ppmbz2 : ppmbz2files) {
			String basename = FilenameUtils.getBaseName(ppmbz2);
			byte[] ppmbytes = Util.readSequenceFileFromHDFS(hadoopMaster
					+ "/colorferet/" + basename + ".seq");
			ImageInputStream iis = ImageIO
					.createImageInputStream(new ByteArrayInputStream(ppmbytes));
			BufferedImage rawimage = PPMImageReader.read(iis);
			List<int[]> faces = OpenCV.detectFace(rawimage);
			logger.debug("faces=" + faces.size());
		}
		stopwatch = new DateTime();
		logger.info("Time (ms) to detect faces from ColorFeret data (*.seq) on HDFS="
				+ (stopwatch.getMillis() - startwatch.getMillis()));

		startwatch = new DateTime();
		for (String ppmbz2 : ppmbz2files) {
			String basename = FilenameUtils.getBaseName(ppmbz2);
			byte[] ppmbytes = Util
					.readSequenceFileFromS3("s3n://ori-colorferet-seq/"
							+ basename + ".seq");
			ImageInputStream iis = ImageIO
					.createImageInputStream(new ByteArrayInputStream(ppmbytes));
			BufferedImage rawimage = PPMImageReader.read(iis);
			List<int[]> faces = OpenCV.detectFace(rawimage);
			logger.debug("faces=" + faces.size());
		}
		stopwatch = new DateTime();
		logger.info("Time (ms) to detect faces from ColorFeret data (*.seq) on S3="
				+ (stopwatch.getMillis() - startwatch.getMillis()));

	}

}
