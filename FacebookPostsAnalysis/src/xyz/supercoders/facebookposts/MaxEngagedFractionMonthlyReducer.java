package xyz.supercoders.facebookposts;

import java.io.IOException;
import java.util.HashSet;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.supercoders.facebookposts.custom.PostDetailsWritable;

public class MaxEngagedFractionMonthlyReducer extends Reducer<IntWritable, PostDetailsWritable, IntWritable, Text> {

	public void reduce(IntWritable _key, Iterable<PostDetailsWritable> values, Context context) throws IOException, InterruptedException {
		boolean visited = false;
		double largest = 0;
		HashSet<String> typeCategorySet = new HashSet<>();
		
		for (PostDetailsWritable val : values) {
			
			String categoryName = getCategoryName(val.getCategory().get());
			
			if(!visited){
				largest = val.getPeopleEngagedFraction().get();
				typeCategorySet.add("("+val.getType()+" "+categoryName+")");
				visited = true;
			}
			else{
				if(val.getPeopleEngagedFraction().get()>largest){
					largest = val.getPeopleEngagedFraction().get();
					typeCategorySet.clear();
					typeCategorySet.add("("+val.getType()+" "+categoryName+")");
				}
				else if(val.getPeopleEngagedFraction().get() == largest){
					typeCategorySet.add("("+val.getType()+" "+categoryName+")");
				}
			}
		}
		String value = StringUtils.join(typeCategorySet, ",")+" - "+largest;
		context.write(_key, new Text(value));
	}

	private String getCategoryName(int categoryId) {
		switch(categoryId){
			case 1:
				return "Action";
			case 2:
				return "Product";
			case 3:
				return "Inspiration";
			default:
				return null;
		}
	}

}
