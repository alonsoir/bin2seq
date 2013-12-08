package com.openresearchinc.hadoop.test;

//credit to code from blog: 
//http://noushinb.blogspot.com/2013/04/reading-writing-hadoop-sequence-files.html

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.junit.Assert;
import org.junit.Test;
import org.mortbay.log.Log;

import ucar.nc2.NetcdfFile;

import com.openresearchinc.hadoop.sequencefile.Util;

public class SequenceFileTest {

	@Test
	public void testNetcdf() throws IOException, NoSuchAlgorithmException {
		Map<Text, byte[]> netcdfsequnce = Util
				.readSequenceFile("file://src/test/resources/1.seq");
		for (Map.Entry<Text, byte[]> entry : netcdfsequnce.entrySet()) {
			NetcdfFile ncFile = NetcdfFile.openInMemory(entry.getKey()
					.toString(), entry.getValue());
			Log.info(ncFile.getDetailInfo());
			Assert.assertNotNull(ncFile);
		}
	}

	@Test
	public void testConfig() {
		Configuration conf = new Configuration();
		Util.listHadoopConfiguration(conf);
	}

	@Test
	public void testWriteSequenceFileFromLocalToHDFS() throws IOException {
		Util.writeToSequenceFile("file://src/test/resources/",
				"hdfs://master:8020/tmp/nc", new GzipCodec());
	}

	@Test
	public void testWriteSequenceFileFromLocalToLocal() throws IOException {
		Util.writeToSequenceFile("file://src/test/resources/1.nc",
				"file://src/test/resources/1.seq", new DefaultCodec());
	}

	@Test
	public void testWriteSequenceFile() {
		try {
			Util.writeToSequenceFile("file:////etc/passwd",
					"hdfs://master:8020/tmp/passwd.seq", new SnappyCodec());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testReadSequenceFile() {

		try {
			Util.readSequenceFile("hdfs://master:8020/tmp/passwd.seq");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
