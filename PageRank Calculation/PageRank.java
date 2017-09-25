package pagerank;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PageRank extends Configured implements Tool {
	
	   private static final Logger LOG = Logger .getLogger( PageRank.class);
	   
	   public static void main(String[] args) throws Exception {
		   int res = ToolRunner.run(new PageRank(), args);
		   System.exit(res);
		 }
	   
	   public int run(String[] args) throws Exception {
		   
		   //Create the job to get title and links  
			Job job = Job.getInstance(getConf(), "PageRank");
			job.setJarByClass(this.getClass());
			//Add input and output file paths
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]+"_Intermediate1"));
			//Set the job to map and reducer functions
			job.setMapperClass(Mapper1.class);
			job.setReducerClass(Reducer1.class);
			//Set the output types for map and reduce phase
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.waitForCompletion(true); 
			  
			//Create the configuration
			Configuration conf1 = getConf(); 
			Path path = new Path(args[1]+"_Intermediate1/part-r-00000");
			FileSystem fs = path.getFileSystem(conf1);
			//Get the number of nodes
			int total_nodes = IOUtils.readLines(fs.open(path)).size();
			conf1.setInt("total_nodes",total_nodes);
			Job job2;
			int i=01;
			
			 //create the job to calculate page rank after performing 10 iterations 
			
			for(;i<10;i++){
				
			   
			  	job2 = Job.getInstance(conf1, "PageRank"+i);
			    job2.setJarByClass(this.getClass());
			    //Set paths from the output of the job1 to job2
			    FileInputFormat.addInputPath(job2, new Path(args[1]+"_Intermediate"+i));
			    FileOutputFormat.setOutputPath(job2, new Path(args[1]+"_Intermediate"+(i+1)));
			    // Set the job to Mapper2 and Reducer2 function 
			    job2.setMapperClass(Mapper2.class);
			    job2.setReducerClass(Reducer2.class);
			    job2.setOutputKeyClass(Text.class);
			    job2.setOutputValueClass(Text.class);
			    job2.waitForCompletion(true);
			     
			} 
			
			//Create the configuration
			Configuration cleanConf = getConf(); 
			cleanConf.set("path", args[1]+"_Intermediate");
			//Creating the job task for the Clean up and sorting
			Job job3 = Job.getInstance(cleanConf, "Clean");
			job3.setJarByClass(this.getClass());
			//Set paths from the output of the job2 to job3
			FileInputFormat.addInputPath(job3, new Path(args[1]+"_Intermediate"+i));
			FileOutputFormat.setOutputPath(job3, new Path(args[1]));
			//Set the job to Mapper3 and Reducer3 function
			job3.setMapperClass(Mapper3.class);
			job3.setReducerClass(Reducer3.class);
			job3.setOutputKeyClass(Text.class);
			job3.setOutputValueClass(Text.class);
			return job3.waitForCompletion(true) ? 0 : 1;
			    
		}


	   // Task -1 Create Link graph
	   //Mapper1 class to create the link Graph
	   
	   public static class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {
		   
		   private static final Pattern titlePattern = Pattern.compile(".*<title>(.*?)</title>.*");
		   
		   private static final Pattern textPattern = Pattern.compile(".*<text.*?>(.*?)</text>.*");
		   
		   private static final Pattern linkPattern = Pattern.compile("\\[\\[(.*?)\\]\\]");
		   		   
		   public void map(LongWritable offset, Text lineText, Context context)
		       throws IOException, InterruptedException {
			   
			   	String line = lineText.toString();
			   	
			   	if(line.length()>0){
			   		StringBuffer buffer1 = new StringBuffer("");
			       	Matcher title = titlePattern.matcher(line); 
			       	if(title.matches()){
			       		buffer1.append(new Text(title.group(1).toString()));
			       	}
			       	
			       	Matcher textMatcher = textPattern.matcher(line);
			       	//Selecting the text with in the title tags
			       	
			       	StringBuffer buffer2 = new StringBuffer("");
			       	//Selecting the text with in text tag
			       	
			       	if(textMatcher.matches())
			       	{
			       		String links = textMatcher.group(1).toString();
			       		Matcher linkMatcher = linkPattern.matcher(links);
			       		//Selecting the links with in the list of links
			       		while(linkMatcher.find())
			       		{
			       			for(int j=0; j <linkMatcher.groupCount(); j++ )
			       			{
			       				// Add the delimiter ####@@@@ between the links 
			       				buffer2.append(linkMatcher.group(j+1).toString()+"####@@@@");
			       				
			   					context.write(new Text(linkMatcher.group(j+1).toString()),new Text(""));
			   					context.write(new Text(buffer1.toString()),new Text(linkMatcher.group(j+1).toString()));
			   			    }			
			       	    }		
			       	}	
			       }

		   }
		   
		 }
	   
	   //Reducer1 class to create the link Graph
	   
	   public static class Reducer1 extends Reducer<Text, Text, Text, Text> {
		   @Override
		    public void reduce(Text key, Iterable<Text> links, Context context)
		    throws IOException, InterruptedException {
			   
			    
			    int total_links = 0;
			    
			    StringBuffer buffer2 = new StringBuffer("");
			    
			    for (Text link : links) {
			  	  if(link.toString().length()>0){
			  		  buffer2.append("####@@@@"+link.toString());
			  		  total_links++;
			  	  }
			    }
			    
			    String value = "Harish@@@@@"+ total_links + buffer2.toString();
			    Text text= new Text(value);
			    context.write(key, text);

		  }
		}
	   
	   //Task-2 Processing the page rank
	   
	   //Mapper2 class to process the Page rank
	   public static class Mapper2 extends Mapper<LongWritable, Text, Text, Text> {
		   
		   public void map(LongWritable offset, Text lineText, Context context)
			       throws IOException, InterruptedException {
			   
			  	 try{
			  		 String line = lineText.toString();
			  		 
			  	     	if(line.length()>0){
			  	     		String[] split1 = line.split("\\t");
			  	     		String[] split2 =split1[1].trim().split("@@@@@");
			  	     		
			  	     		if(split2.length>1){
			  	     			
			  	     			if(split2[0].equals("Harish")){
			      	     			int total_nodes =Integer.parseInt( context.getConfiguration().get("total_nodes"));
			      	     			context.write(new Text(split1[0]), new Text(((1/(double)total_nodes))+""));
			      	     		}
			      	     		else{
			      	     			double page_rank = Double.parseDouble(split2[0].trim());
				      	     		String[] links = split2[1].split("####@@@@");
				      	     		int no_of_links = Integer.parseInt(links[0].trim());
				      	     		if(no_of_links>0){
				      	     			double pageR = (double)page_rank/no_of_links;
				      	     			for(int i = 1;i<links.length;i++){
				      	     				context.write(new Text(links[i]), new Text(pageR +""));
				      	     				}
				      	     			}
			      	     		
			      	     		}
			      	     		context.write(new Text(split1[0]), new Text(split1[1]));
			  	     		}	
			  	     	}
			  	 }catch(Exception e){
			  		 e.printStackTrace();
			  	 }

			   	
			   }

		 }
	   
	   //Reducer2 class to process the page rank
	   
	   public static class Reducer2 extends Reducer<Text, Text, Text, Text> {
		   
		   @Override
		   public void reduce(Text key, Iterable<Text> links, Context context)
		       throws IOException, InterruptedException {
			   
			   	double pageRank = 0;
			   	double damping_factor = 0.85;
			   	StringBuffer buffer1= new StringBuffer("");
			    StringBuffer buffer2 = new StringBuffer("");
			      //    
			     for (Text link : links) {
			   	  if(link.toString().length()>0){
			   	     // Split the link using delimiter	
			   		  if(link.toString().contains("@@@@@")){
			   		      
			   			 String str[] = link.toString().split("@@@@@");
			   			 buffer1.append(str[0]);
			   			 buffer2.append(str[1]);
			   		  }
			   		  else{
			   			  pageRank = pageRank + Double.parseDouble(link.toString());
			   		  }

			   	  }
			     } 
			     pageRank = pageRank +(1-damping_factor);
			     String value = pageRank+"@@@@@"+buffer2.toString();
			     context.write(key, new Text(value));

		   }

		 } 


	   //Task- 3  Performing cleanup and sort
	   
	   //Mapper3 class to Perform cleanup and sort
	   public static class Mapper3 extends Mapper<LongWritable, Text, Text, Text> {
		   
		    public void map(LongWritable offset, Text lineText, Context context)
			        throws IOException, InterruptedException {
		    	
		    	  String line = lineText.toString();
		    	  if(line.length()>0){
			    		String[] split1 =  line.split("\\t");
			    		//Split the line using the delimiter "@@@@@"
			    		String[] split2 =  split1[1].trim().split("@@@@@");
			    		context.write(new Text("sorting"), new Text(split1[0]+"####@@@@"+split2[0]));

		    	  }

			    }

		  }
	   
	   
	   ////Reducer3 class to Perform cleanup and sort
	   public static class Reducer3 extends Reducer<Text, Text, Text, Text> {
		    @Override
		    public void reduce(Text key, Iterable<Text> links, Context context)
		        throws IOException, InterruptedException {
		    	
			      //Getting the path from getConfiguration function
			  	  String path= context.getConfiguration().get("path");
			  	  FileSystem fs = FileSystem.get(context.getConfiguration());
			  	  //Deleting the _Intermediate files
			  	  for(int i=1;i<=10;i++){
			  		  fs.delete(new Path(path+i),true);
			  	  }
			  	  //Sort the pages according to descending order in the rank 
			  	  ArrayList<Rank> page_rank = new ArrayList<Rank>();
			  	
			    	for (Text link : links) {
			    		  if(link.toString().length()>0){
			    		  if(link.toString().contains("####@@@@")){
			    			 String str[] = link.toString().split("####@@@@");
			    			 
			    			 double rank = Double.parseDouble(str[1].trim());
			    			 Rank r = new Rank(str[0],rank);
			    			 page_rank.add(r);
			    		  }
			    	  }  
			    	}
			    	Collections.sort(page_rank, new Comparator<Rank>() {
	                    public int compare(Rank r1, Rank r2) {
	                        return Double.compare(r2.getRank(), r1.getRank());
	                    }
	                });
			    	for(int i=0; i< page_rank.size();i++){
			    		Rank r1 = page_rank.get(i);
			    		String rank_text = r1.getRank()+"";
			    		context.write(new Text(r1.getTitle()), new Text(rank_text));
			    	
			    	}
		    	

		    }


		   }

}