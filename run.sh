mvn clean compile package -DskipTests=true
export HDFSOUT="/output"
export MVNJARS=`find $HOME/.m2/repository/ -name "*.jar" |while read jar; do echo $jar; done |tr '\n' ','`
export LIBJARS="$MVNJARS,$HADOOP_HOME/lib/native"
export HADOOP_CLASSPATH=`echo ${LIBJARS} | sed s/,/:/g`
export CLASSPATH=`echo ${LIBJARS} | sed s/,/:/g`

#Processing the file compute pr mean/min/max
hadoop fs -rm -f -r /output
hadoop jar ./target/bin2seq-0.1.jar com.openresearchinc.hadoop.sequencefile.NetCDF -libjars ${LIBJARS} hdfs:///nc-seq hdfs:///output
exit

#Passed: copy a nasanex netcdf file to HDFS
hadoop fs -rm -f -r /nc-seq
hadoop jar ./target/bin2seq-0.1.jar com.openresearchinc.hadoop.sequencefile.Util -libjars $LIBJARS -in s3://nasanex/NEX-DCP30/BCSD/rcp26/mon/atmos/pr/r1i1p1/v1.0/CONUS/pr_amon_BCSD_rcp26_r1i1p1_CONUS_HadGEM2-ES_200512-200512.nc -ext nc -out hdfs:///nc-seq -codec snappy
exit

#Passed: Identify tempo from *.h5.seq
hadoop fs -rm -f -r /output
hadoop jar ./target/bin2seq-0.1.jar com.openresearchinc.hadoop.sequencefile.HDF5 -libjars ${LIBJARS} hdfs:///msd10k-seq/  /output -attr tempo
exit

#Passed: Copy small set of MSD *.h5 from S3 to HDFS
hadoop fs -rm -f -r /msd10k-seq
hadoop jar ./target/bin2seq-0.1.jar com.openresearchinc.hadoop.sequencefile.Util -libjars $LIBJARS -in s3:///ori-msd10k-h5/data/A/A/A/ -ext h5 -out hdfs:///msd10k-seq -codec snappy
exit 

#Passed: Copy small set of ppm.bz from S3 to HDFS 
hadoop fs -rm -f -r /colorferet-seq
hadoop jar ./target/bin2seq-0.1.jar com.openresearchinc.hadoop.sequencefile.Util -libjars $LIBJARS -in s3://ori-colorferetsubset/00001/ -ext ppm.bz2 -out hdfs:///colorferet-seq -codec snappy 
exit

#Passed: Open CV Face Detection 
hadoop fs -rm -f -r /output
hadoop jar ./target/bin2seq-0.1.jar com.openresearchinc.hadoop.sequencefile.OpenCV -libjars $LIBJARS hdfs:///colorferet-seq/ hdfs:///output  
exit





#passed 

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

