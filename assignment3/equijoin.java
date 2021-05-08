import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.*;

public class equijoin {
    public static void main(String [] args) throws Exception
    {
        Configuration c=new Configuration();
        String[] files=new GenericOptionsParser(c,args).getRemainingArgs();
        Path input=new Path(files[0]);
        Path output=new Path(files[1]);
        Job j=new Job(c, "equijoin");
        j.setJarByClass(equijoin.class);
        j.setMapperClass(MapForEquijoin.class);
        j.setReducerClass(ReduceForEquijoin.class);
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(Text.class);
        j.setOutputKeyClass(NullWritable.class);
        j.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);
        System.exit(j.waitForCompletion(true)?0:1);
    }

    public static class MapForEquijoin extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context con) throws IOException, InterruptedException {
            String tuple = value.toString();
            // Set 2nd column i.e. join column as output key
            // Entire line as output value
            String[] fields = tuple.split(", ");
            Text outputKey = new Text(fields[1]);
            Text outputValue = new Text(tuple);
            con.write(outputKey, outputValue);
        }
    }

    public static class ReduceForEquijoin extends Reducer<Text, Text, NullWritable, Text> {
        public void reduce(Text joinCol, Iterable<Text> tuples, Context con) throws IOException, InterruptedException {
            // Concatenate tuples if they are from different tables
            List<String> tupleList = getList(tuples);

            if(tupleList.size() > 1) {
                for(int i=0; i<tupleList.size(); i++) {
                    for(int j=i+1; j<tupleList.size(); j++) {
                        String t1 = tupleList.get(i);
                        String[] f1 = t1.split(", ");
                        String tname1 = f1[0];
                        String t2 = tupleList.get(j);
                        String[] f2 = t2.split(", ");
                        String tname2 = f2[0];


                        if(!tname1.equals(tname2)) {
                            String joinedTuple = t1 + ", " + t2;
                            con.write(NullWritable.get(), new Text(joinedTuple));
                        }
                    }
                }
            }
        }

        public List<String> getList(Iterable<Text> tuples) {
            List<String> tupleList = new ArrayList<String>();
            for(Text t : tuples) {
                tupleList.add(t.toString());
            }

            return tupleList;
        }
    }
}