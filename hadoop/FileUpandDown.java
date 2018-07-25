package hadoop;
import java.io.IOException; 
import java.net.URI; 
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.FileSystem; 
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
public class FileUpandDown {
	
	public void main(String args[]) throws URISyntaxException, IOException{
	
	}
	//�ϴ�
	@Before
	public void init(){
		
	}
	@Test
	public void Up() throws URISyntaxException, IOException { 
	Configuration conf = new Configuration() ; 
	URI uri = new URI("hdfs://localhost:9005") ; 
	FileSystem file = FileSystem.get(uri, conf) ; 
	Path path1 = new Path("E://hadoop/SAData.txt"); 
	Path path2 = new Path("hdfs://localhost:9005/input_2015081124/") ; 
	if(!(file.exists(path2))){ 
		file.mkdirs(path2); 
	} 
	file.copyFromLocalFile(path1, path2); 
	file.close(); 
	}
	
	//����
	@Test
	public void Down() throws URISyntaxException, IOException{
	    Configuration conf = new Configuration() ;
	    URI uri = new URI("hdfs://localhost:9005") ;
	    FileSystem file = FileSystem.get(uri, conf);
	    //�����ļ���Դ��ַ��Ŀ�ĵ�ַ����
	    Path src = new Path("hdfs://localhost:9005/outs/part-r-00000") ;
	    Path dst = new Path("E://hadoop/medel.txt") ;
	    if(file.exists(src)){
	    	file.copyToLocalFile(src, dst);
	    }else{
	        System.out.println("�ļ������ڣ�");
	    }
	    file.close();
	}
	@Test
	public void test(){
		String s=new String("��	��ѽ ѽ �� Ҫ �� �� �� ô ô ô �� ֻ �� ���� �� �� �� �� �� ��Ϊ ���� �� �� �� �� �� �� �� ");
		s=s.replaceAll("[\\u4e00-\\u9fa5]", "");
		System.out.println(s);
	}
}
