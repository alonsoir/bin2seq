mvn clean compile package -DskipTests=true
export LIBJARS="$HOME/.m2/repository/com/amazonaws/aws-java-sdk/1.0.002/aws-java-sdk-1.0.002.jar,$HOME/.m2/repository/org/hdfgroup/hdf-java/2.6.1/hdf-java-2.6.1.jar,$HOME/.m2/repository/edu/ucar/netcdf/4.2.20/netcdf-4.2.20.jar,$HOME/.m2/repository/local/opencv/2.4.8/opencv-2.4.8.jar,$HOME/.m2/repository/com/googlecode/javacv/javacv/0.7/javacv-0.7.jar,$HOME/.m2/repository/com/googlecode/javacpp/javacpp/0.7/javacpp-0.7.jar,$HOME/javacv-bin/javacv-linux-x86_64.jar,$HOME/javacv-cppjars/ffmpeg-2.1.1-linux-x86_64.jar,/home/heq/javacv-cppjars/opencv-2.4.8-windows-x86_64.jar"
export HADOOP_CLASSPATH=`echo ${LIBJARS} | sed s/,/:/g`
export HADOOP_OPTS="$HADOOP_OPTS -Djava.library.path=/home/heq/opencv/release/lib/"
#export JAVA_LIBRARY_PATH="/home/heq/opencv/release/lib/"
export CLASSPATH=`echo ${LIBJARS} | sed s/,/:/g`
#echo $CLASSPATH
#hadoop jar ./target/bin2seq-0.1.jar com.openresearchinc.hadoop.sequencefile.OpenCV -in s3://ori-tmp/lena.png.seq -libjars $LIBJARS -Djava.library.path=/home/heq/opencv/release/lib/
#hadoop jar ./target/bin2seq-0.1.jar com.openresearchinc.hadoop.sequencefile.OpenCV -in s3://ori-tmp/lena.png.seq -libjars $LIBJARS 
hadoop jar ./target/bin2seq-0.1.jar com.openresearchinc.hadoop.sequencefile.OpenCV -in hdfs://master:8020/tmp/lena.png.seq -libjars $LIBJARS



