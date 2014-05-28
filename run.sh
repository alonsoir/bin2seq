mvn clean compile package -DskipTests=true
hadoop fs -rmr /output
hadoop fs -rmr /colorferetsub
export MVNJARS=`find $HOME/.m2/repository/ -name "*.jar" |while read jar; do echo $jar; done |tr '\n' ','`
export LIBJARS="$MVNJARS,$JAVACV_BIN_HOME/javacv.jar,$JAVACV_BIN_HOME/javacpp.jar,$JAVACV_BIN_HOME/javacv-linux-x86_64.jar,$JAVACV_CPP_HOME/opencv-2.4.8-linux-x86_64.jar,$JAVACV_CPP_HOME/ffmpeg-2.1.1-linux-x86_64.jar"
export HADOOP_CLASSPATH=`echo ${LIBJARS} | sed s/,/:/g`
export CLASSPATH=`echo ${LIBJARS} | sed s/,/:/g`

#passed test cases
#hadoop jar ./target/bin2seq-0.1.jar com.openresearchinc.hadoop.sequencefile.Util -pack -in s3://ori-msd10k-h5/data/A/A/ -ext h5 -out /msd10k-seq -codec snappy
hadoop jar ./target/bin2seq-0.1.jar com.openresearchinc.hadoop.sequencefile.Util -list -in hdfs:///msd10k-seq/1.seq

#failed test case
#hadoop jar ./target/bin2seq-0.1.jar com.openresearchinc.hadoop.sequencefile.OpenCV -file s3://ori-tmp/lena.png.seq -libjars $LIBJARS -Djava.library.path=/home/heq/opencv/release/lib/
#hadoop jar ./target/bin2seq-0.1.jar com.openresearchinc.hadoop.sequencefile.OpenCV -file s3n://ori-tmp/lena.png.seq -libjars $LIBJARS 
#hadoop jar ./target/bin2seq-0.1.jar com.openresearchinc.hadoop.sequencefile.OpenCV -file hdfs://master:8020/tmp/lena.png.seq -libjars $LIBJARS
#hadoop jar ./target/bin2seq-0.1.jar com.openresearchinc.hadoop.sequencefile.OpenCV -file hdfs://master:8020/tmp/00001_930831_fa_a.ppm.seq -libjars $LIBJARS
#hadoop jar ./target/bin2seq-0.1.jar com.openresearchinc.hadoop.sequencefile.OpenCV -dir s3n://ori-colorferet-seq/ -ext ppm.seq -libjars $LIBJARS
#hadoop jar ./target/bin2seq-0.1.jar com.openresearchinc.hadoop.sequencefile.OpenCV -dir hdfs:///colorferet-seq/ -ext ppm.seq -libjars $LIBJARS
#hadoop jar ./target/bin2seq-0.1.jar com.openresearchinc.hadoop.sequencefile.OpenCV  -libjars $LIBJARS /colorferetsub /output
#hadoop jar ./target/bin2seq-0.1.jar com.openresearchinc.hadoop.sequencefile.HDF5 -libjars ${LIBJARS} /tmp/TRAXLZU12903D05F94.h5.seq /output -attr similar_artist
#hadoop jar ./target/bin2seq-0.1.jar com.openresearchinc.hadoop.sequencefile.NetCDF -libjars ${LIBJARS} /tmp/ncar.nc.seq /output 
#hadoop jar ./target/bin2seq-0.1.jar com.openresearchinc.hadoop.sequencefile.Util -pack -in s3://ori-colorferetsubset/00001/ -ext ppm.bz2 -out /tmp1 -codec snappy

