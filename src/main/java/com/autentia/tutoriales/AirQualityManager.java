package com.autentia.tutoriales;

import java.io.IOException;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AirQualityManager extends Configured implements Tool {

	private static final String MEASURE_TYPE = "measureType";

	private static final int DATE_ORDER = 0;

	private static final int PROVINCE_ORDER = 10;

	private static final String SEPARATOR = ";";

	public static class AirQualityMapper extends Mapper<Object, Text, MeasureWritable, FloatWritable> {

		private static final String DATE_SEPARATOR = "/";
		private String measureType;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			this.measureType = context.getConfiguration().get(MEASURE_TYPE);
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			final String[] values = value.toString().split(SEPARATOR);
			final MeasureWritable measure = getMeasure(values, measureType);
			final String measureValue = format(values[MeasureType.getOrder(measureType)]);

			if (measure != null && NumberUtils.isNumber(measureValue)) {
				context.write(measure, new FloatWritable(Float.valueOf(measureValue)));
			}
		}

		private MeasureWritable getMeasure(String[] values, String measureType) {
			MeasureWritable measureWritable = null;

			final String date = format(values[DATE_ORDER]);

			if (isValidData(date)) {
				final String year = date.split(DATE_SEPARATOR)[2];
				final String province = format(values[PROVINCE_ORDER]);

				measureWritable = new MeasureWritable(year, province);
			}

			return measureWritable;
		}

		private boolean isValidData(final String date) {
			return date.contains(DATE_SEPARATOR);
		}

		private String format(String value) {
			return value.trim();
		}
	}

	public static class AirQualityReducer extends Reducer<MeasureWritable, FloatWritable, MeasureWritable, FloatWritable> {

		public void reduce(MeasureWritable key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
			float maxMeasure = 0f;
			for (FloatWritable measureValue : values) {
				maxMeasure = Math.max(maxMeasure, measureValue.get());
			}

			context.write(key, new FloatWritable(maxMeasure));
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		if (args.length != 3) {
			System.err.println("AirQualityManager required params: <input file> <output dir> <measure type>");
			System.exit(2);
		}

		deleteOutputFileIfExists(args);

		final Configuration configuration = new Configuration();
		configuration.set(MEASURE_TYPE, args[2]);

		final Job job = new Job(configuration);

		job.setJarByClass(AirQualityManager.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(MeasureWritable.class);
		job.setMapOutputValueClass(FloatWritable.class);
		job.setOutputKeyClass(MeasureWritable.class);
		job.setOutputValueClass(FloatWritable.class);

		job.setMapperClass(AirQualityMapper.class);
		job.setReducerClass(AirQualityReducer.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

		return 0;
	}

	private void deleteOutputFileIfExists(String[] args) throws IOException {
		final Path output = new Path(args[1]);
		FileSystem.get(output.toUri(), getConf()).delete(output, true);
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new AirQualityManager(), args);
	}
}
