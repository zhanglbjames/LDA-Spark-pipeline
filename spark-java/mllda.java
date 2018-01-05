/**
* @Title: mllda.java
* @Package lda
* @Description: TODO:
* @author zss
* @date 2016��10��31��
* @version V1.0
*/
package lda;

/**
* @ClassName: mllda
* @Description: TODO:
* @author zss
* @date 2016��10��31��
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

    SparkConf conf = new SparkConf().setAppName("mllda").setMaster("local");//���õ�������
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
     * ���ò���
     * 
     * 
     * */
    corpus.cache();
    int topic_num = 4;//���������
    int dict_num = 100;//�ֵ�Ĵ�С
    int item_num = 10;//ȡǰnum���������Ĵ�д���ļ���
    // Cluster the documents into three topics using LDA
    LDAModel ldaModel = new LDA().setK(topic_num).run(corpus);

    
    
    //�򿪰���100���ʵ��ֵ��ļ�
    File hundredWords = new File("C:\\Users\\zss-manong\\Desktop\\wordtest\\hundred_AVG_TFIDF_dict.csv");
    BufferedReader br=new BufferedReader(new InputStreamReader(new FileInputStream(hundredWords),"utf-8"));
    
    String[] wordsArr = new String[dict_num];
    
    //��100���ʶ�����������
    for(int i=0;i<dict_num;i++){
    	wordsArr[i] = br.readLine();
    }
    //ȫ��������ļ�
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
    
  //10�������ʵĴ�������ļ�
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
    //����������
    double[][] out_matrix = new double[topic_num][ldaModel.vocabSize()];
    Matrix topics = ldaModel.topicsMatrix();
    for (int topic = 0; topic < topic_num; topic++) {
      for (int word = 0; word < ldaModel.vocabSize(); word++) {
    	  out_matrix[topic][word] = topics.apply(word, topic);
      }
    }
    
    //�Ծ������ȫ���򣬲�����ԭ���Ĵʵ�˳��
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
    
    
    
    //��ȫ������д�뵽�ļ�
    for(int i=0;i<topic_num;i++){
    	String line = "���� "+Integer.toString(i+1)+" :";
    	for(int j=0;j<dict_num;j++){
    		line += Double.toString(out_matrix[i][j])+"% ["+wordsArr[index[i][j]]+"] ";
    	}
    	line += "\n";
    	bw.write(line);
    }
  //������10�������ʵľ���д�뵽�ļ�
    for(int i=0;i<topic_num;i++){
    	String line = "���� "+Integer.toString(i+1)+" :";
    	for(int j=0;j<item_num;j++){
    		line += Double.toString(out_matrix[i][j])+"% ["+wordsArr[index[i][j]]+"] ";
    	}
    	line += "\n";
    	h_bw.write(line);
    }
    
    //�ر��ļ���
    bw.close();
    h_bw.close();
    br.close();
    
    //��ʾ�����Ϣ
    System.out.println("finished!");
    
    jsc.stop();
  }
}
