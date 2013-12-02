package com.openresearchinc.hadoop.test;

//credit to code from blog: 
//http://noushinb.blogspot.com/2013/04/reading-writing-hadoop-sequence-files.html

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.junit.Test;

import ucar.nc2.NetcdfFile;

import com.openresearchinc.hadoop.sequencefile.Util;

public class SequenceFileTest {

	@Test
	public void testNetcdf() throws IOException {
		Map<Text, byte[]> netcdfsequnce = Util
				.readSequenceFile("hdfs://master:8020/tmp/nc1.seq");
		for (Map.Entry<Text, byte[]> entry : netcdfsequnce.entrySet()) {
			NetcdfFile ncFile = NetcdfFile.openInMemory(entry.getKey()
					.toString(), entry.getValue());
		}
	}

	@Test
	public void testConfig() {
		Configuration conf = new Configuration();
		Util.listHadoopConfiguration(conf);
	}

	@Test
	public void testWriteSequenceFile() {
		try {
			Util.writeToSequenceFile("file:///etc/passwd",
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
