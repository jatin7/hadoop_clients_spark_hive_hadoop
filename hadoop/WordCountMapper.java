package hadoop;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper 
        extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        
    	//把类标拿出来
    	String str = value.toString();
    	String str1=null;
    	str1=str.substring(0, str.indexOf("\t"));
    	str=str.substring(str.indexOf("\t")+1);
    	
    	//过滤非中文
    	String[] arr=str.split(" ");
    	StringBuffer sb = new StringBuffer();
    	for(int i=0; i<arr.length; i++){
			if(arr[i].matches("[\\u4e00-\\u9fa5]+")){     //如果全是中文
    			sb. append(str1+"-"+arr[i]+" ");
    		}
    	}
    	str=sb.toString();
    	//按空格分隔
    	StringTokenizer wordIter = new StringTokenizer(str);
        while (wordIter.hasMoreTokens()) {
            word.set(wordIter.nextToken());
            if(word.getLength()!=0){
            	context.write(word, one);
            }
        }
        context.write(new Text(str1), one);
    }
}
