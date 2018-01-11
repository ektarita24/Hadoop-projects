package xyz.supercoders.facebookposts;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.supercoders.facebookposts.custom.PostDetailsWritable;

public class MaxEngagedFractionMonthlyCombiner extends Reducer<IntWritable, PostDetailsWritable, IntWritable, PostDetailsWritable> {

	public void reduce(IntWritable _key, Iterable<PostDetailsWritable> values, Context context) throws IOException, InterruptedException {
		
		boolean visited = false;
		double largest = 0;
		HashSet<PostDetailsWritable> postDetailsWritableSet = new HashSet<>();
		
		for (PostDetailsWritable val : values) {
			String type = val.getType().toString();
			int category = val.getCategory().get();
			double peopleEngagedFraction = val.getPeopleEngagedFraction().get();
			
			if(!visited){
				largest = val.getPeopleEngagedFraction().get();
				postDetailsWritableSet.add(new PostDetailsWritable(new Text(type),new IntWritable(category),new DoubleWritable(peopleEngagedFraction)));
				visited = true;
			}
			else{
				if(val.getPeopleEngagedFraction().get()>largest){
					largest = val.getPeopleEngagedFraction().get();
					postDetailsWritableSet.clear();
					postDetailsWritableSet.add(new PostDetailsWritable(new Text(type),new IntWritable(category),new DoubleWritable(peopleEngagedFraction)));
				}
				else if(val.getPeopleEngagedFraction().get() == largest){
					postDetailsWritableSet.add(new PostDetailsWritable(new Text(type),new IntWritable(category),new DoubleWritable(peopleEngagedFraction)));
				}
			}
		}
		Iterator<PostDetailsWritable> itr = postDetailsWritableSet.iterator();
		while(itr.hasNext()){
			PostDetailsWritable postDetailsWritable = itr.next();
			context.write(_key, postDetailsWritable);
		}
	}

}
