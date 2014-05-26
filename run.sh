mvn clean compile package -DskipTests=true
hadoop fs -rmr /output
export LIBJARS=`find $HOME/.m2/repository/ -name "*.jar" |while read jar; do echo $jar; done |tr '\n' ','`
export HADOOP_CLASSPATH=`echo ${LIBJARS} | sed s/,/:/g`
export CLASSPATH=`echo ${LIBJARS} | sed s/,/:/g`

#passed test cases
hadoop jar ./target/bin2seq-0.1.jar com.openresearchinc.hadoop.sequencefile.Util -pack -in s3://ori-haarcascade/ -ext xml -out /output -codec snappy
hadoop jar ./target/bin2seq-0.1.jar com.openresearchinc.hadoop.sequencefile.Util -list -in hdfs:///output 

#failed test case
#hadoop jar ./target/bin2seq-0.1.jar com.openresearchinc.hadoop.sequencefile.OpenCV -file s3://ori-tmp/lena.png.seq -libjars $LIBJARS -Djava.library.path=/home/heq/opencv/release/lib/
#hadoop jar ./target/bin2seq-0.1.jar com.openresearchinc.hadoop.sequencefile.OpenCV -file s3n://ori-tmp/lena.png.seq -libjars $LIBJARS 
#hadoop jar ./target/bin2seq-0.1.jar com.openresearchinc.hadoop.sequencefile.OpenCV -file hdfs://master:8020/tmp/lena.png.seq -libjars $LIBJARS
#hadoop jar ./target/bin2seq-0.1.jar com.openresearchinc.hadoop.sequencefile.OpenCV -file hdfs://master:8020/tmp/00001_930831_fa_a.ppm.seq -libjars $LIBJARS
#hadoop jar ./target/bin2seq-0.1.jar com.openresearchinc.hadoop.sequencefile.OpenCV -dir s3n://ori-colorferet-seq/ -ext ppm.seq -libjars $LIBJARS
#hadoop jar ./target/bin2seq-0.1.jar com.openresearchinc.hadoop.sequencefile.OpenCV -dir hdfs:///colorferet-seq/ -ext ppm.seq -libjars $LIBJARS
#hadoop jar ./target/bin2seq-0.1.jar com.openresearchinc.hadoop.sequencefile.OpenCV -libjars ${LIBJARS}  /colorferet/1.seq /output
#hadoop jar ./target/bin2seq-0.1.jar com.openresearchinc.hadoop.sequencefile.HDF5 -libjars ${LIBJARS} /tmp/TRAXLZU12903D05F94.h5.seq /output -attr similar_artist
#hadoop jar ./target/bin2seq-0.1.jar com.openresearchinc.hadoop.sequencefile.NetCDF -libjars ${LIBJARS} /tmp/ncar.nc.seq /output 
#hadoop jar ./target/bin2seq-0.1.jar com.openresearchinc.hadoop.sequencefile.Util -pack -in s3://ori-colorferetsubset/00001/ -ext ppm.bz2 -out /tmp1 -codec snappy

