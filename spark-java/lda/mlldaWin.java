/**
* @Title: mllda.java
* @Package lda
* @Description: TODO:
* @author zss
* @date 2016年10月31日
* @version V1.0
*/
package lda;

/**
* @ClassName: mllda
* @Description: TODO:
* @author zss
* @date 2016年10月31日
*
*/
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

// $example on$
import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.DistributedLDAModel;
import org.apache.spark.mllib.clustering.LDA;
import org.apache.spark.mllib.clustering.LDAModel;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
// $example off$

public class mllda {
  public static void main(String[] args) throws Exception {

    SparkConf conf = new SparkConf().setAppName("mllda").setMaster("local");//设置单机运行
    JavaSparkContext jsc = new JavaSparkContext(conf);

    // $example on$
    // Load and parse the data
    String path = "file:///C:/Users/zss-manong/Desktop/wordtest/AVG_TFIDF_matrix_words.csv";
    JavaRDD<String> data = jsc.textFile(path);
    JavaRDD<Vector> parsedData = data.map(
      new Function<String, Vector>() {
        public Vector call(String s) {
          String[] sarray = s.trim().split(" ");
          double[] values = new double[sarray.length];
          for (int i = 0; i < sarray.length; i++) {
            values[i] = Double.parseDouble(sarray[i]);
          }
          return Vectors.dense(values);
        }
      }
    );
    // Index documents with unique IDs
    JavaPairRDD<Long, Vector> corpus =
      JavaPairRDD.fromJavaRDD(parsedData.zipWithIndex().map(
        new Function<Tuple2<Vector, Long>, Tuple2<Long, Vector>>() {
          public Tuple2<Long, Vector> call(Tuple2<Vector, Long> doc_id) {
            return doc_id.swap();
          }
        }
      )
    );
    
    
    
    /*
     * 配置参数
     * 
     * 
     * */
    corpus.cache();
    int topic_num = 4;//主题的数量
    int dict_num = 100;//字典的大小
    int item_num = 10;//取前num个概率最大的词写到文件中
    // Cluster the documents into three topics using LDA
    LDAModel ldaModel = new LDA().setK(topic_num).run(corpus);

    
    
    //打开包含100个词的字典文件
    File hundredWords = new File("C:\\Users\\zss-manong\\Desktop\\wordtest\\hundred_AVG_TFIDF_dict.csv");
    BufferedReader br=new BufferedReader(new InputStreamReader(new FileInputStream(hundredWords),"utf-8"));
    
    String[] wordsArr = new String[dict_num];
    
    //将100个词读出到数组中
    for(int i=0;i<dict_num;i++){
    	wordsArr[i] = br.readLine();
    }
    //全部排序的文件
    File topicFileAllWords = new File("C:\\Users\\zss-manong\\Desktop\\wordtest\\topic_matrix_all_words.csv");
    BufferedWriter bw = null;
    
    if(topicFileAllWords.exists()){
		try{
			topicFileAllWords.delete();
			topicFileAllWords.createNewFile();
			bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(topicFileAllWords), "utf-8")); 
		}
		catch(IOException e){e.printStackTrace();}	
	}
	else{
		try{
			topicFileAllWords.createNewFile();
			bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(topicFileAllWords), "utf-8"));
		}
		catch(IOException e){e.printStackTrace();}
	}
    
  //10个最大概率的词排序的文件
    File hundredtopicFileAllWords = new File("C:\\Users\\zss-manong\\Desktop\\wordtest\\topic_matrix_hundred_words.csv");
    BufferedWriter h_bw = null;
    if(hundredtopicFileAllWords.exists()){
		try{
			hundredtopicFileAllWords.delete();
			hundredtopicFileAllWords.createNewFile();
			h_bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(hundredtopicFileAllWords), "utf-8"));
		}
		catch(IOException e){e.printStackTrace();}	
	}
	else{
		try{
			hundredtopicFileAllWords.createNewFile();
			h_bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(hundredtopicFileAllWords), "utf-8"));
		}
		catch(IOException e){e.printStackTrace();}
	}
    
    
    // Output topics. Each is a distribution over words (matching word count vectors)
    //输出主题矩阵
    double[][] out_matrix = new double[topic_num][ldaModel.vocabSize()];
    Matrix topics = ldaModel.topicsMatrix();
    for (int topic = 0; topic < topic_num; topic++) {
      for (int word = 0; word < ldaModel.vocabSize(); word++) {
    	  out_matrix[topic][word] = topics.apply(word, topic);
      }
    }
    
    //对矩阵进行全排序，并保留原来的词典顺序
    int[][] index = new int[topic_num][ldaModel.vocabSize()];
 
    for (int topic = 0; topic < topic_num; topic++) {
        for (int word = 0; word < ldaModel.vocabSize()-1; word++) {
      	  double change = out_matrix[topic][word];
      	  int index_word =word;
      	  for(int i =word+1;i<ldaModel.vocabSize();i++){
      		  if(change < out_matrix[topic][i]){
      			  change = out_matrix[topic][i];
      			  index_word = i;
      		  }
      	  }
      	out_matrix[topic][index_word] = out_matrix[topic][word];
      	out_matrix[topic][word] = change;
      	index[topic][word] = index_word;
        }
      }
    
    
    
    //将全部矩阵写入到文件
    for(int i=0;i<topic_num;i++){
    	String line = "主题 "+Integer.toString(i+1)+" :";
    	for(int j=0;j<dict_num;j++){
    		line += Double.toString(out_matrix[i][j])+"% ["+wordsArr[index[i][j]]+"] ";
    	}
    	line += "\n";
    	bw.write(line);
    }
  //将包含10个最大概率的矩阵写入到文件
    for(int i=0;i<topic_num;i++){
    	String line = "主题 "+Integer.toString(i+1)+" :";
    	for(int j=0;j<item_num;j++){
    		line += Double.toString(out_matrix[i][j])+"% ["+wordsArr[index[i][j]]+"] ";
    	}
    	line += "\n";
    	h_bw.write(line);
    }
    
    //关闭文件流
    bw.close();
    h_bw.close();
    br.close();
    
    //提示完成信息
    System.out.println("finished!");
    
    jsc.stop();
  }
}
