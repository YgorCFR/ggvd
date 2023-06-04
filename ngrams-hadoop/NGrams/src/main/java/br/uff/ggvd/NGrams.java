package br.uff.ggvd;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class NGrams {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text>{

        private final static Text one = new Text("1");
        private Text word = new Text();
        private List wordList = new ArrayList();
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), " ");
            while (itr.hasMoreTokens()) {
                wordList.add(itr.nextToken());
            }

        }

        private void writeInsideContextWithCleanupConfigurations(Context context, StringBuffer wordSet) throws IOException, InterruptedException {
            String originPath = new Path(context.getInputSplit().toString()).getName();
            String originFileName = originPath.substring(0, originPath.indexOf(":"));
            wordSet.append("|file:" + originFileName);
            word.set(wordSet.toString());
            context.write(word, one);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            int grams = Integer.parseInt(context.getConfiguration().get("N"));
            StringBuffer wordSet = new StringBuffer("");
            List tempIndexList = new ArrayList();
            List tempIndexCopiesList = new ArrayList();
            for (int i = 0; i < wordList.size() - grams; i ++) {
                int k = i;
                tempIndexList.addAll(IntStream.rangeClosed(k, k + grams - 1).boxed().collect(Collectors.toList()));
                tempIndexCopiesList.addAll(Collections.nCopies(grams, i));
            }
            int firstWordOfRound = 0;
            for (int j = 0; j < tempIndexList.size(); j ++) {
                int indexValue = ((Integer) tempIndexList.get(j)).intValue();
                if (firstWordOfRound > 0) {
                    wordSet.append(" ");
                    wordSet.append(wordList.get(indexValue));
                } else {
                    wordSet.append(wordList.get(indexValue));
                }
                firstWordOfRound++;
                if (1 + j < tempIndexList.size()) {
                    int indexCopyValue = ((Integer) tempIndexCopiesList.get(j)).intValue();
                    int nextIndexCopyValue = ((Integer) tempIndexCopiesList.get(j + 1)).intValue();
                    if (indexCopyValue != nextIndexCopyValue) {
                        writeInsideContextWithCleanupConfigurations(context, wordSet);
                        wordSet = new StringBuffer("");
                        firstWordOfRound = 0;
                    }
                }
                else if (j == tempIndexList.size() - 1) {
                    writeInsideContextWithCleanupConfigurations(context, wordSet);
                }
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,Text,Text,Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            int minCount = Integer.parseInt(context.getConfiguration().get("CountMin"));
            int sum = 0;
            for (Text val : values) {
                sum += Integer.valueOf(val.toString());
            }
            if (sum >= minCount) {
                String outputValue = Integer.toString(sum) + "  " + key.toString().substring(key.toString().lastIndexOf("|file:") + 6);
                Text finalKey = new Text(key.toString().substring(0, key.toString().indexOf("|file:")));
                result.set(outputValue);
                context.write(finalKey, result);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // Configuring arguments
        conf.set("N", args[0]);
        conf.set("CountMin", args[1]);
        conf.set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
        );
        conf.set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName()
        );
        // Setting hadoop application parameters
        FileSystem fs = FileSystem.get(conf);
        Job job = Job.getInstance(conf, "NGrams");

        job.setJarByClass(NGrams.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // Input path parameter
        FileInputFormat.addInputPath(job, new Path(args[2]));
        // Deleting output path
        fs.delete(new Path(args[3]), true);
        // Output path parameter
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}