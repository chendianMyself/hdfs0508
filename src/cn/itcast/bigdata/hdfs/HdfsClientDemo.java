package cn.itcast.bigdata.hdfs;

import java.net.URI;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;
/**
 * 
 * 瀹㈡埛绔幓鎿嶄綔hdfs鏃讹紝鏄湁涓�涓敤鎴疯韩浠界殑
 * 榛樿鎯呭喌涓嬶紝hdfs瀹㈡埛绔痑pi浼氫粠jvm涓幏鍙栦竴涓弬鏁版潵浣滀负鑷繁鐨勭敤鎴疯韩浠斤細-DHADOOP_USER_NAME=hadoop
 * 
 * 涔熷彲浠ュ湪鏋勯�犲鎴风fs瀵硅薄鏃讹紝閫氳繃鍙傛暟浼犻�掕繘鍘�
 * @author
 *
 */
public class HdfsClientDemo {
	FileSystem fs = null;
	Configuration conf = null;
	@Before
	public void init() throws Exception{
		
		conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.60.141:9000");
		
		//鎷垮埌涓�涓枃浠剁郴缁熸搷浣滅殑瀹㈡埛绔疄渚嬪璞�
		/*fs = FileSystem.get(conf);*/
		//鍙互鐩存帴浼犲叆 uri鍜岀敤鎴疯韩浠�
		fs = FileSystem.get(new URI("hdfs://192.168.60.141:9000"),conf,"hadoop"); //鏈�鍚庝竴涓弬鏁颁负鐢ㄦ埛鍚�
	}

	@Test
	public void testUpload() throws Exception {
		
		Thread.sleep(2000);
		fs.copyFromLocalFile(new Path("E:/access.log"), new Path("/access.log.copy"));
		fs.close();
	}
	
	
	@Test
	public void testDownload() throws Exception {
		
		fs.copyToLocalFile(new Path("/access.log.copy"), new Path("d:/"));
		fs.close();
	}
	
	@Test
	public void testConf(){
		Iterator<Entry<String, String>> iterator = conf.iterator();
		while (iterator.hasNext()) {
			Entry<String, String> entry = iterator.next();
			System.out.println(entry.getValue() + "--" + entry.getValue());//conf鍔犺浇鐨勫唴瀹�
		}
	}
	
	/**
	 * 鍒涘缓鐩綍
	 */
	@Test
	public void makdirTest() throws Exception {
		boolean mkdirs = fs.mkdirs(new Path("/aaa/bbb"));
		System.out.println(mkdirs);
	}
	
	/**
	 * 鍒犻櫎
	 */
	@Test
	public void deleteTest() throws Exception{
		boolean delete = fs.delete(new Path("/aaa"), true);//true锛� 閫掑綊鍒犻櫎
		System.out.println(delete);
	}
	
	@Test
	public void listTest() throws Exception{
		
		FileStatus[] listStatus = fs.listStatus(new Path("/"));
		for (FileStatus fileStatus : listStatus) {
			System.err.println(fileStatus.getPath()+"================="+fileStatus.toString());
		}
		//浼氶�掑綊鎵惧埌鎵�鏈夌殑鏂囦欢
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
		while(listFiles.hasNext()){
			LocatedFileStatus next = listFiles.next();
			String name = next.getPath().getName();
			Path path = next.getPath();
			System.out.println(name + "---" + path.toString());
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		//鎷垮埌涓�涓枃浠剁郴缁熸搷浣滅殑瀹㈡埛绔疄渚嬪璞�
		FileSystem fs = FileSystem.get(conf);
		
		fs.copyFromLocalFile(new Path("G:/access.log"), new Path("/access.log.copy"));
		fs.close();
	}
	

}
