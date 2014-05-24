package com.openresearchinc.hadoop.test;

import java.io.IOException;
import java.util.Date;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;

import com.openresearchinc.hadoop.sequencefile.Util;

public class BaseTest {
	final static Logger logger = Logger.getLogger(BaseTest.class);

	String hadoopMaster;
	Date start, end;

	@Before
	public void setUp() throws Exception {
		hadoopMaster = Util.getHadoopMasterURI();
		start = new Date();// timing the execution
	}

	@After
	public void tearDown() throws IOException {
		end = new Date();
		long duration = end.getTime() - start.getTime();
		logger.info("duration =" + duration + " ms");
	}
}
