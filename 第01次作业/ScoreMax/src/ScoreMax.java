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

public class ScoreMax {

    public static class ScoreMapper
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

            String[] parts = line.split(",");

            if (parts.length < 3) {
                return;
            }

            String course = parts[0].trim();
            String student = parts[1].trim();

            int maxScore = Integer.MIN_VALUE;

            for (int i = 2; i < parts.length; i++) {
                try {
                    int score = Integer.parseInt(parts[i].trim());

                    if (score > maxScore) {
                        maxScore = score;
                    }
                } catch (NumberFormatException ignored) {
                }
            }

            if (maxScore != Integer.MIN_VALUE) {
                outKey.set(course);
                outValue.set(student + "," + maxScore);

                context.write(outKey, outValue);
            }
        }
    }

    public static class ScoreReducer
            extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(
                Text key,
                Iterable<Text> values,
                Context context)
                throws IOException, InterruptedException {

            int maxScore = Integer.MIN_VALUE;
            List<String> maxStudents = new ArrayList<>();

            for (Text value : values) {

                String[] parts = value.toString().split(",");

                if (parts.length != 2) {
                    continue;
                }

                String student = parts[0];
                int score = Integer.parseInt(parts[1]);

                if (score > maxScore) {
                    maxScore = score;
                    maxStudents.clear();
                    maxStudents.add(student);
                } else if (score == maxScore) {
                    maxStudents.add(student);
                }
            }

            for (String student : maxStudents) {
                context.write(
                        key,
                        new Text(student + "\t" + maxScore)
                );
            }
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: ScoreMax <input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(
                conf,
                "Max Score By Course"
        );

        job.setJarByClass(ScoreMax.class);

        job.setMapperClass(ScoreMapper.class);
        job.setReducerClass(ScoreReducer.class);

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