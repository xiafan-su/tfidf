# Make sure we have all the arguments
if [ $# -ne 5 ]; then
   printf "./run_tfidf.sh <input_dir> <output_dir> [conf] num\n"
   printf "   input_dir  = HDFS Directory with the input files\n"
   printf "   output_dir = HDFS Directory to place the output\n"
   printf "   conf       = Hadoop configuration options\n"
   printf "   songnum    = Num of song"
   exit -1
fi

# Get the input data
declare INPUT=$1;
declare OUTPUT=$2;
declare CONFIG=$3;
declare SONGNUM=$4;
declare REDUCENUM=$5;

# Remove the output directories
${HADOOP_HOME}/bin/hadoop fs -rmr $OUTPUT/tfidf1 >& /dev/null
${HADOOP_HOME}/bin/hadoop fs -rmr $OUTPUT/tfidf2 >& /dev/null
${HADOOP_HOME}/bin/hadoop fs -rmr $OUTPUT/tfidf3 >& /dev/null
${HADOOP_HOME}/bin/hadoop fs -rmr $OUTPUT/tfidf4 >& /dev/null

# Execute the jobs
printf "\nExecuting Job 1: Word Frequency in Doc\n"
${HADOOP_HOME}/bin/hadoop jar tfidf.jar tf-idf-1 $CONFIG $INPUT $OUTPUT/tfidf1 $REDUCENUM

printf "\nExecuting Job 2: Word Counts For Docs\n"
${HADOOP_HOME}/bin/hadoop jar tfidf.jar tf-idf-2 $CONFIG $OUTPUT/tfidf1 $OUTPUT/tfidf2 $REDUCENUM

printf "\nExecuting Job 3: Docs In Corpus and TF-IDF\n"
${HADOOP_HOME}/bin/hadoop jar tfidf.jar tf-idf-3 $CONFIG $INPUT $OUTPUT/tfidf2 $OUTPUT/tfidf3 $SONGNUM $REDUCENUM

printf "\nExecuting Job 4: Select keyword by TF-IDF\n"
${HADOOP_HOME}/bin/hadoop jar tfidf.jar tf-idf-4 $CONFIG $OUTPUT/tfidf3 $OUTPUT/tfidf4 $REDUCENUM

