import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

public class FavoriteMax {

    public static class MaxMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        private final Text outKey = new Text();
        private final Text outValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();

            if (line.isEmpty()) {
                return;
            }

            String[] parts = line.split("\\s+");

            if (parts.length < 3) {
                return;
            }

            String year = parts[0];
            String itemId = parts[1];
            String count = parts[2];

            outKey.set(year);
            outValue.set(itemId + "," + count);

            context.write(outKey, outValue);
        }
    }

    public static class MaxReducer
            extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(
                Text key,
                Iterable<Text> values,
                Context context)
                throws IOException, InterruptedException {

            int maxCount = -1;
            List<String> maxItems = new ArrayList<>();

            for (Text value : values) {

                String[] parts = value.toString().split(",");

                String itemId = parts[0];
                int count = Integer.parseInt(parts[1]);

                if (count > maxCount) {
                    maxCount = count;
                    maxItems.clear();
                    maxItems.add(itemId);
                } else if (count == maxCount) {
                    maxItems.add(itemId);
                }
            }

            for (String itemId : maxItems) {
                context.write(
                        key,
                        new Text(itemId + "\t" + maxCount)
                );
            }
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println(
                    "Usage: FavoriteMax <input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(
                conf,
                "Favorite Max By Year"
        );

        job.setJarByClass(FavoriteMax.class);

        job.setMapperClass(MaxMapper.class);
        job.setReducerClass(MaxReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(
                job,
                new Path(args[0])
        );

        FileOutputFormat.setOutputPath(
                job,
                new Path(args[1])
        );

        System.exit(
                job.waitForCompletion(true) ? 0 : 1
        );
    }
}