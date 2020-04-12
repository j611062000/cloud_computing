import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;  


/*
Author: 107522112 Sid Lin
Description: There are 3 function will be implemented, mapper, combiner and reducer.
FilePath: user/s107522115/lab1/practice2/output1
Output
  mapper   => (word:fileName, 1)
  combiner => (word, fileName:frequency)
  reducer  => (word, fileName_1:Frequency_1...fileName_n:Frequency_n)
*/

public class InvertedIndex {

  private static String deli = ":";

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    private static Text aggregatedKey = new Text();
    private final static Text oneValue = new Text("1");

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

      while (itr.hasMoreTokens()) {
        aggregatedKey.set(String.format("%s%s%s", itr.nextToken(), deli, fileName));
        context.write(aggregatedKey, oneValue);
      }
    }
  }

  public static class Combiner extends Reducer<Text,Text,Text,Text> {
    
    Text textWrapper = new Text();
    
    public void reduce(Text key, Iterable<Text> oneValues, Context context)  
            throws IOException, InterruptedException {
      
      int frequency = 0;

      // Aggregate by the key of "word:fileName", i.e the (Text) key passed from arguments
      for(Text oneValue: oneValues) {
        frequency += Integer.valueOf(oneValue.toString());
      }
      
      String aggregatedKey[] = key.toString().split(deli);
      key.set(aggregatedKey[0]);
      textWrapper.set(String.format("%s:%d", aggregatedKey[1], frequency));
      context.write(key, textWrapper);
    }

  }

  public static class FileIndexReducer
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      
      StringBuffer buffer = new StringBuffer();

      for (Text value : values) {
        buffer.append(String.format("%s:", value));
      }
      result.set(buffer.toString());
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Lab1_Practice2: Index Inverted");
    
    job.setJarByClass(InvertedIndex.class);
    
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(Combiner.class);
    job.setReducerClass(FileIndexReducer.class);

    job.setMapOutputKeyClass(Text.class);  
    job.setMapOutputValueClass(Text.class); 

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
