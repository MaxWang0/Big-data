package FindUserReview;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;





import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class FindUserReview {

	//The Mapper classes and  reducer  code
	
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		private HashMap<String, String> mymap = new HashMap<String, String>();
		@SuppressWarnings("resource")
		//static String movieid;		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
				String[] mydata = value.toString().split("::"); //				System.out.println(value.toString());
				String userid=mydata[8];
				String businessid =mydata[2];
				String review = mydata[1];
				if(mymap.get(businessid) != null)
				{
					
					context.write(new Text(userid), new Text(review));
				}
			}

		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
				// TODO Auto-generated method stu
				super.setup(context); 
				Configuration conf = context.getConfiguration();
				//movieid = conf.get("movieid"); //for retrieving data you set in driver code
				Path[] localPaths = context.getLocalCacheFiles();
				for(Path myfile:localPaths)
				{
					String line=null;
					String nameofFile=myfile.getName();
					File file =new File(nameofFile+"");
					FileReader fr= new FileReader(file);
					BufferedReader br= new BufferedReader(fr);
					line=br.readLine();
					while(line!=null)
					{
						String[] arr=line.split("::");
						if(arr[3].contains("Stanford")){
							mymap.put(arr[2], arr[3]); //businessid and fulladdress
						}

						line=br.readLine();
					}
				}
		}
	}
	

	//The reducer class	
	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text v : values) {
				context.write(key, v);
			}

		}
	}

	//Driver code
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err
					.println("Usage: findusers <review> <business>  <output>");
			System.exit(2);
		}
		//conf.set("movieid", otherArgs[3]);
		Job job = new Job(conf, "joinexc");
		String filep = otherArgs[1];
		job.addCacheFile(new URI(filep + "/" + "business.csv"));
		job.setJarByClass(FindUserReview.class);
		job.setReducerClass(Reduce.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]),
				TextInputFormat.class, Map.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		job.waitForCompletion(true);

	}

}


