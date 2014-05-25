mvn clean compile package -DskipTests=true
hadoop fs -rmr /output
export LIBJARS="$HOME/.m2/repository/org/apache/ant/ant/1.9.4/ant-1.9.4.jar,\
$HOME/.m2/repository/com/amazonaws/aws-java-sdk/1.7.9/aws-java-sdk-1.7.9.jar,\
$HOME/.m2/repository/org/hdfgroup/hdf-java/2.6.1/hdf-java-2.6.1.jar,\
$HOME/.m2/repository/edu/ucar/netcdf/4.2/netcdf-4.2.jar,\
$HOME/.m2/repository/org/apache/commons/commons-lang3/3.3.2/commons-lang3-3.3.2.jar,\
$HOME/.m2/repository/org/apache/httpcomponents/httpclient/4.3.3/httpclient-4.3.3.jar,\
$HOME/.m2/repository/org/apache/httpcomponents/httpcore/4.3.2/httpcore-4.3.2.jar,\
$HOME/.m2/repository/com/fasterxml/jackson/core/jackson-databind/2.3.3/jackson-databind-2.3.3.jar,\
$HOME/.m2/repository/com/fasterxml/jackson/core/jackson-core/2.3.3/jackson-core-2.3.3.jar,\
$HOME/.m2/repository/com/fasterxml/jackson/core/jackson-annotations/2.3.0/jackson-annotations-2.3.0.jar,\
$HOME/.m2/repository/commons-io/commons-io/2.4/commons-io-2.4.jar,\
$JAVACV_BIN_HOME/javacv.jar,$JAVACV_BIN_HOME/javacpp.jar,$JAVACV_BIN_HOME/javacv-linux-x86_64.jar,\
$JAVACV_CPP_HOME/opencv-2.4.8-linux-x86_64.jar,$JAVACV_CPP_HOME/ffmpeg-2.1.1-linux-x86_64.jar"
export HADOOP_CLASSPATH=`echo ${LIBJARS} | sed s/,/:/g`
export CLASSPATH=`echo ${LIBJARS} | sed s/,/:/g`

#passed test cases
hadoop jar ./target/bin2seq-0.1.jar com.openresearchinc.hadoop.sequencefile.Util -pack -in s3://ori-colorferetsubset/00001/ -ext ppm.bz2 -out /output -codec snappy

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

