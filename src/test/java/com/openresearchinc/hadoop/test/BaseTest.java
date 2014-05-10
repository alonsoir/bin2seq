package com.openresearchinc.hadoop.test;

import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.xml.sax.InputSource;

public class BaseTest {
	final static Logger logger = Logger.getLogger(BaseTest.class);

	String hadoopMaster;
	Date start, end;

	@Before
	public void setUp() throws Exception {

		File hadoopHomeDir = new File(System.getenv("HADOOP_HOME") + "/etc/hadoop");
		if (hadoopHomeDir.isDirectory()) {
			File[] files = hadoopHomeDir.listFiles(new FileFilter() {
				public boolean accept(File file) {
					return file.getName().endsWith("core-site.xml");
				}
			});
			if (files.length == 1) {
				try {
					XPathFactory xpf = XPathFactory.newInstance();
					XPath xpath = xpf.newXPath();
					XPathExpression xpe = xpath.compile("//property[name/text()='fs.default.name']/value");
					InputSource coresitexml = new InputSource(new FileReader(files[0]));
					hadoopMaster = xpe.evaluate(coresitexml);
					logger.debug(hadoopMaster);
				} catch (IOException ioe) {
					logger.error("Cannot find core-site.xml under $HADOOP_HOME");
					System.exit(1);
				} catch (XPathExpressionException e) {
					logger.error("Error when parsing core-site.xml under $HADOOP_HOME");
					System.exit(1);
				}
			} else {
				logger.error("Cannot find fs.default.name in core-site.xml");
				System.exit(1);
			}
		} else {
			logger.error("$HADOOP_HOME is NOT defeined");
			System.exit(1);
		}

		// timing the execution
		start = new Date();
	}

	@After
	public void tearDown() throws IOException {
		end = new Date();
		long duration = end.getTime() - start.getTime();
		logger.info("duration =" + duration + " ms");
	}
}
