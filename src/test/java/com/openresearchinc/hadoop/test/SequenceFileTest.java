package com.openresearchinc.hadoop.test;

//credit to code from blog: 
//http://noushinb.blogspot.com/2013/04/reading-writing-hadoop-sequence-files.html

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class SequenceFileTest {

	@Autowired
	HadoopClient hadoopClient;

	String sequenceFileName = "/tmp/nb.sgz";
	String hadoopLocalFS = "file:///";
	String hadoopRemoteFS = "hdfs://stage-hadoop01:8020";

	@Test
	public void testConfig() {
		Configuration conf = new Configuration();
		HadoopClient.listHadoopConfiguration(conf);
	}

	@Test
	public void testWriteSequenceFile() {
		String dataFileName = "/tmp/test.txt";

		try {
			int numOfLines = 20;
			String baseStr = "....Test...";
			List<String> lines = new ArrayList<String>();
			for (int i = 0; i < numOfLines; i++)
				lines.add(i + baseStr + UUID.randomUUID());

			File dataFile = new File(dataFileName);
			FileUtils.writeLines(dataFile, lines, true);
			Thread.sleep(2000);
			HadoopClient.writeToSequenceFile(dataFile, sequenceFileName,
					hadoopLocalFS);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testReadSequenceFile() {

		try {
			HadoopClient.readSequenceFile(sequenceFileName, hadoopLocalFS);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testCopySequenceFileToRemoteHDFS() {
		String tempFileName = "/tmp/local-test.txt";
		String sequenceFileName = "/tmp/seqfile-record-compressed.sgz";
		String hadoopLocalFS = "file:///";
		String hadoopRemoteFS = "hdfs://stage-hadoop01:8020";

		try {
			int numOfLines = 5;
			String baseStr = "....Test...";
			List<String> lines = new ArrayList<String>();
			for (int i = 0; i < numOfLines; i++)
				lines.add(i + baseStr + UUID.randomUUID());

			File dataFile = new File(tempFileName);
			FileUtils.writeLines(dataFile, lines, true);
			Thread.sleep(2000);
			HadoopClient.writeToSequenceFile(dataFile, sequenceFileName,
					hadoopLocalFS);
			HadoopClient.readSequenceFile(sequenceFileName, hadoopLocalFS);
			HadoopClient.copySequenceFile(sequenceFileName, sequenceFileName,
					hadoopRemoteFS);
			HadoopClient.readSequenceFile(sequenceFileName, hadoopRemoteFS);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
