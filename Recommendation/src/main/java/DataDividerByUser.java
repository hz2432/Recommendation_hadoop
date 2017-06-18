import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import javax.security.auth.login.Configuration;
import java.io.IOException;

/**
 * Created by haiki on 6/13/17.
 */
public class DataDividerByUser {
    public static class DataDivdermapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        //map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // input user, movies, rating
            String[] user_movie_rating = value.toString().trim().split(",");
            int userID = Integer.parseInt(user_movie_rating[0]);
            String movieID = user_movie_rating[1];
            String rating = user_movie_rating[2];
            
            context.write(new IntWritable(userID), new Text(movieID + ":" + rating));
        }
        
        public static class DataDriverReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
            //reduce method
            @Override
            public void reduce(IntWritable key, Iterable<Text> values, Mapper.Context context) throws IOException, InterruptedException {
                //key = userID
                //outputValue = list of movies
                StringBuilder sb = new StringBuilder();
                while(values.iterator().hasNext()){
                    sb.append("," + values.iterator().next());
                }
                //output sb = ,movie1:2, movie2:3
                context.write(key, new Text(sb.toString().replaceFirst(",", "")));
            }
            
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        
    }
}
