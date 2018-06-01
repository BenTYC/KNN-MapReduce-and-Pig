package edu.ucr.cs.cs226.tchen063;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * Hello world!
 *
 */
public class KNN {

	static private int K = 7;
	static private double qX = 51.8219827;
	static private double qY = 31.9436557;
	
	public static class IdDistance implements WritableComparable<IdDistance> {
		String id;
		Double distance;

		public void set(Double distance, String id) {
			this.id = id;
			this.distance = distance;
		}

		public Double getDistance() {
			return distance;
		}

		public String getId() {
			return id;
		}

		public void write(DataOutput out) throws IOException {
			out.writeDouble(distance);
			out.writeUTF(id);
		}

		public void readFields(DataInput in) throws IOException {
			distance = in.readDouble();
			id = in.readUTF();
		}

		@Override
		public String toString() {
			return String.format("%d: %f", id, distance);
		}

		public int compareTo(IdDistance o) {
			// TODO Auto-generated method stub
			return (this.distance).compareTo(o.distance);
		}
	}
	
	public static class KnnMapper extends Mapper<Object, Text, NullWritable, IdDistance> {

		IdDistance outVal = new IdDistance();
		TreeMap<Double, String> KnnMap = new TreeMap<Double, String>();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] parts = value.toString().split(",");
			String id = parts[0];
			double x = Double.parseDouble(parts[1]);
			double y = Double.parseDouble(parts[2]);
			Double distance = Math.sqrt((qX - x) * (qX - x) + (qY - y) * (qY - y));
			KnnMap.put(distance, id);
			
			System.out.println(KnnMap);
		}
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException{
			for(Map.Entry<Double, String> entry : KnnMap.entrySet())
			{
				  Double knnDist = entry.getKey();
				  String knnId = entry.getValue();
				  outVal.set(knnDist, knnId);
				  context.write(NullWritable.get(), outVal);
			}
		}
	}
	public static class KnnReducer extends Reducer<NullWritable, IdDistance, NullWritable, Text>{
	
		TreeMap<Double, String> KnnMap = new TreeMap<Double, String>();
		
		@Override
		public void reduce(NullWritable key, Iterable<IdDistance> values, Context context) throws IOException, InterruptedException
		{
			for (IdDistance val : values){
				String id = val.getId();
				double tDist = val.getDistance();

				KnnMap.put(tDist, id);
				if (KnnMap.size() > K)
					KnnMap.remove(KnnMap.lastKey());
			}
			context.write(NullWritable.get(), new Text(KnnMap.toString()));
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "KNN");
		job.setJarByClass(KNN.class);
		
		// Setup MapReduce job
		job.setMapperClass(KnnMapper.class);
		job.setReducerClass(KnnReducer.class);
		job.setNumReduceTasks(1); // Only one reducer in this design

		// Specify key / value
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(IdDistance.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
				
		// Input (the data file) and Output (the resulting classification)
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// Execute job and return status
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
