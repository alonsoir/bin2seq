package com.openresearchinc.hadoop.test;

//Also credit goes to code from blog: 
//http://noushinb.blogspot.com/2013/04/reading-writing-hadoop-sequence-files.html

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import ncsa.hdf.object.h5.H5File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.junit.Test;

import ucar.ma2.Array;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import com.openresearchinc.hadoop.sequencefile.Util;
import com.openresearchinc.hadoop.sequencefile.hdf5_getters;

public class SequenceFileTest {

	@Test
	// Test support (indirect) open HDF5 file from memory using netcdf API
	// TODO: python
	// http://stackoverflow.com/questions/16654251/can-h5py-load-a-file-from-a-byte-array-in-memory
	public void testNetcdfToH5() throws Exception {
		File file = new File(this.getClass()
				.getResource("/TRAXLZU12903D05F94.h5").getPath());

		H5File h5 = hdf5_getters.hdf5_open_readonly(this.getClass()
				.getResource("/TRAXLZU12903D05F94.h5").getPath());
		double h5_temp = hdf5_getters.get_tempo(h5);

		byte[] netcdfinbyte = FileUtils.readFileToByteArray(file);
		NetcdfFile netCDFfile = NetcdfFile.openInMemory("inmemory.h5",
				netcdfinbyte);// use any dummy filename for file in memory
		Variable var = (Variable) netCDFfile
				.findVariable("/analysis/songs.tempo");
		Array content = var.read();// 1D array
		double netcdf_tempo = content.getDouble(0); // 1 column only

		assertEquals(h5_temp, netcdf_tempo, 0.001);
	}

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
