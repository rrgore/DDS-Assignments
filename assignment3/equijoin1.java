//import java

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

//importing from hadoop Library

public class equijoin
{
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = getObject();

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static Job getObject() throws IOException
    {
        Configuration config = new Configuration();
        Job job = new Job(config, "equijoin");
        job.setJarByClass(equijoin.class);

        job.setMapperClass(CustomizedMapper.class);

        job.setReducerClass(CustomizedReducer.class);

        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Object.class);
        job.setOutputValueClass(Text.class);
        return job;
    }

    public static class CustomizedMapper extends Mapper<LongWritable, Text, DoubleWritable, Text>
    {
        private DoubleWritable resultKey = new DoubleWritable();
        private Text results = new Text();
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer tuplesIterations = new StringTokenizer(value.toString(), "\n");
            while (tuplesIterations.hasMoreTokens()) {
                String tuple = tuplesIterations.nextToken();
                StringTokenizer compsItrs = new StringTokenizer(tuple, ", ");
                String tableName = compsItrs.nextToken();
                resultKey.set(Double.parseDouble(compsItrs.nextToken()));
                results.set(tuple);
                context.write(resultKey, results);
            }
        }
    }

    public static class CustomizedReducer extends Reducer<DoubleWritable, Text, Object, Text>
    {

        public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Text opText = new Text();
            StringBuilder outputStringBuilder = new StringBuilder();
            String t1 = null;
            List<String> tuples = new ArrayList<String>();
            List<String> tuplesTable1 = new ArrayList<String>();
            List<String> tuplesTable2 = new ArrayList<String>();
            for (Text value : values) {
                tuples.add(value.toString());
            }
            if(tuples.size() < 2) {
                return;
            } else {
                t1 = tuples.get(0).split(", ")[0];
                for (String tuple : tuples) {
                    if (t1.equals(tuple.split(", ")[0])) {
                        tuplesTable1.add(tuple);
                    } else {
                        tuplesTable2.add(tuple);
                    }
                }
                if(tuplesTable1.size() == 0 || tuplesTable2.size() == 0) {
                    return;
                }else {
                    for (String table1Tuple : tuplesTable1) {
                        for (String table2Tuple : tuplesTable2) {
                            outputStringBuilder.append(table1Tuple).append(", ").append(table2Tuple).append("\n");
                        }
                    }
                    opText.set(outputStringBuilder.toString().trim());
                    context.write(null, opText);
                }
            }
        }
    }
}
