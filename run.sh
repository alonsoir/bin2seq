mvn clean compile package -DskipTests=true
export Ver="0.1"
export MVNJARS=`find $HOME/.m2/repository/ -name "*.jar" |while read jar; do echo $jar; done |tr '\n' ','`
export LIBJARS="$MVNJARS,$HADOOP_HOME/lib/native"
export HADOOP_CLASSPATH=`echo ${LIBJARS} | sed s/,/:/g`
export CLASSPATH=`echo ${LIBJARS} | sed s/,/:/g`
export HEAPSIZE="10240"
export CMD="hadoop jar ./target/bin2seq-${Ver}.jar" 

#Passed: Simple test case to retrieve geotiff coordinate corners  
hadoop fs -rm -f -r /output
HADOOP_HEAPSIZE=$HEAPSIZE $CMD com.openresearchinc.hadoop.sequencefile.GeoTiff -libjars ${LIBJARS} hdfs:///landsat-seq hdfs:///output
exit

#Passed: copy nasanex Landsat geotif to HDFS
hadoop fs -rm -f -r /landsat-seq
HADOOP_HEAPSIZE=$HEAPSIZE $CMD com.openresearchinc.hadoop.sequencefile.Util -libjars $LIBJARS -in s3://nasanex/Landsat/gls/1975/001/026/ -ext tif -out hdfs:///landsat-seq -codec snappy
exit

#Passed: Processing the file compute pr mean/min/max
hadoop fs -rm -f -r /output
HADOOP_HEAPSIZE=$HEAPSIZE $CMD com.openresearchinc.hadoop.sequencefile.NetCDF -libjars ${LIBJARS} hdfs:///nc-seq hdfs:///output
exit

#Passed: copy a nasanex netcdf file to HDFS
hadoop fs -rm -f -r /nc-seq
HADOOP_HEAPSIZE=$HEAPSIZE $CMD com.openresearchinc.hadoop.sequencefile.Util -libjars $LIBJARS -in s3://nasanex/NEX-DCP30/BCSD/rcp26/mon/atmos/pr/r1i1p1/v1.0/CONUS/pr_amon_BCSD_rcp26_r1i1p1_CONUS_HadGEM2-ES_200512-200512.nc -ext nc -out hdfs:///nc-seq -codec snappy
exit

#Passed: Identify tempo from *.h5.seq
hadoop fs -rm -f -r /output
HADOOP_HEAPSIZE=$HEAPSIZE $CMD com.openresearchinc.hadoop.sequencefile.HDF5 -libjars ${LIBJARS} hdfs:///msd10k-seq/  /output -attr tempo
exit

#Passed: Copy small set of MSD *.h5 from S3 to HDFS
hadoop fs -rm -f -r /msd10k-seq
HADOOP_HEAPSIZE=$HEAPSIZE $CMD com.openresearchinc.hadoop.sequencefile.Util -libjars $LIBJARS -in s3:///ori-msd10k-h5/data/A/A/A/ -ext h5 -out hdfs:///msd10k-seq -codec snappy
exit 

#Passed: Copy small set of ppm.bz from S3 to HDFS 
hadoop fs -rm -f -r /colorferet-seq
HADOOP_HEAPSIZE=$HEAPSIZE $CMD com.openresearchinc.hadoop.sequencefile.Util -libjars $LIBJARS -in s3://ori-colorferetsubset/00001/ -ext ppm.bz2 -out hdfs:///colorferet-seq -codec snappy 
exit

#Passed: Open CV Face Detection 
hadoop fs -rm -f -r /output
HADOOP_HEAPSIZE=$HEAPSIZE $CMD com.openresearchinc.hadoop.sequencefile.OpenCV -libjars $LIBJARS hdfs:///colorferet-seq/ hdfs:///output  
exit
