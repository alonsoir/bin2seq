package com.openresearchinc.hadoop.test;

import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.log4j.Logger;
import org.junit.Test;

import com.openresearchinc.hadoop.sequencefile.OpenCV;
import com.openresearchinc.hadoop.sequencefile.Util;

public class Benchmarks extends BaseTest {
	final static Logger logger = Logger.getLogger(Benchmarks.class);

	// @format off
	/**
	 * Prepare colorferet dataset in sequencefile format in both s3:// and
	 * hdfs://
	 * 
	 * @throws Exception
	 */
	// @format on
	@Test
	public void CopyColorferetUntarToHDFS() throws Exception {
		List<String> ppmbz2files = Util.listFiles("s3://ori-colorferet-untar/", "ppm.bz2");
		logger.info("total files=" + ppmbz2files.size());
		for (String ppmbz2 : ppmbz2files) {
			String basename = FilenameUtils.getBaseName(ppmbz2);
			Util.writeToSequenceFile(ppmbz2, hadoopMaster + "/colorferet/" + basename + ".seq", new SnappyCodec());
			Util.writeToSequenceFile(ppmbz2, "s3n://ori-colorferet-seq/" + basename + ".seq", new SnappyCodec());
		}
	}

	/**
	 * Benchmark Face detection of all NIST colorferet database stored as
	 * SequenceFile on S3
	 * 
	 * @throws Exception
	 */
	@Test
	public void ColorferetFaceDetectionFromS3() throws Exception {
		OpenCV.detectFacesInDir("s3n://ori-colorferet-seq/", "ppm.seq");
	}

	/**
	 * Benchmark Face detection of all NIST colorferet database stored as
	 * SequenceFile on HDFS
	 * 
	 * @throws Exception
	 */
	@Test
	public void ColorferetFaceDetectionFromHDFS() throws Exception {
		OpenCV.detectFacesInDir(hadoopMaster + "/colorferet/", "ppm.seq");
	}

}
