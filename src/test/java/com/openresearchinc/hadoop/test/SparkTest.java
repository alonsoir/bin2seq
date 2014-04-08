package com.openresearchinc.hadoop.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.Test;

import scala.Serializable;
import scala.Tuple2;

public class SparkTest implements Serializable {
	private final static long serialVersionUID = -3319354077527132831L;
	private final static Logger logger = Logger.getLogger(SparkTest.class);

	/**
	 * Test search text within a cached JavaRDD data from local file system Need
	 * to define system environment of SPARK_HOME in ~/.profile or in Eclipse
	 */
	@Test
	public void testLocal() {
		String logFile = this.getClass().getResource("/log4j.properties")
				.getPath();

		JavaSparkContext sc = new JavaSparkContext("local",
				"JavaSparkLocalSearch", System.getenv("SPARK_HOME"),
				JavaSparkContext.jarOfClass(this.getClass()));

		JavaRDD<String> logData = sc.textFile(logFile).cache();

		long numAs = logData.filter(new Function<String, Boolean>() {
			private static final long serialVersionUID = 1L;

			public Boolean call(String s) {
				return s.contains("a");
			}
		}).count();

		long numBs = logData.filter(new Function<String, Boolean>() {
			private static final long serialVersionUID = 1L;

			public Boolean call(String s) {
				return s.contains("b");
			}
		}).count();

		logger.info("Lines with a: " + numAs + ", lines with b: " + numBs);

	}

	@Test
	public void testPi() {
		JavaSparkContext jsc = new JavaSparkContext("local", "JavaSparkPi",
				System.getenv("SPARK_HOME"), JavaSparkContext.jarOfClass(this
						.getClass()));

		int slices = 10;
		int n = 100000 * slices;
		List<Integer> l = new ArrayList<Integer>(n);
		for (int i = 0; i < n; i++) {
			l.add(i);
		}

		JavaRDD<Integer> dataSet = jsc.parallelize(l, slices);

		int count = dataSet.map(new Function<Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer integer) {
				double x = Math.random() * 2 - 1;
				double y = Math.random() * 2 - 1;
				return (x * x + y * y < 1) ? 1 : 0;
			}
		}).reduce(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer integer, Integer integer2) {
				return integer + integer2;
			}
		});

		logger.info("Pi is roughly " + 4.0 * count / n);
	}

	@Test
	public void testWordCount() {
		final Pattern SPACE = Pattern.compile(" ");
		String logFile = this.getClass().getResource("/log4j.properties")
				.getPath();

		JavaSparkContext ctx = new JavaSparkContext("local", "JavaWordCount",
				System.getenv("SPARK_HOME"), JavaSparkContext.jarOfClass(this
						.getClass()));

		JavaRDD<String> lines = ctx.textFile(logFile, 1);

		JavaRDD<String> words = lines
				.flatMap(new FlatMapFunction<String, String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<String> call(String s) {
						return Arrays.asList(SPACE.split(s));
					}
				});

		JavaPairRDD<String, Integer> ones = words
				.map(new PairFunction<String, String, Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Integer> call(String s) {
						return new Tuple2<String, Integer>(s, 1);
					}
				});

		JavaPairRDD<String, Integer> counts = ones
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(Integer i1, Integer i2) {
						return i1 + i2;
					}
				});

		List<Tuple2<String, Integer>> output = counts.collect();
		for (Tuple2<?, ?> tuple : output) {
			System.out.println(tuple._1() + ": " + tuple._2());
		}
	}

}
