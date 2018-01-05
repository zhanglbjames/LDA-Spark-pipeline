/**
* @Title: mllda_hdfs.java
* @Package lda
* @Description: TODO:
* @author zss
* @date 2016��11��6��
* @version V1.0
*/
package lda;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.LDA;
import org.apache.spark.mllib.clustering.LDAModel;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
* @ClassName: mllda_hdfs
* @Description: TODO:
* @author zss
* @date 2016��11��6��
*
*/
public class mllda_hdfs {
	 public static void main(String[] args) throws Exception {

		    SparkConf conf = new SparkConf().setAppName("mllda");//.setMaster("local");//���õ�������
		    JavaSparkContext jsc = new JavaSparkContext(conf);

		    // $example on$
		    // Load and parse the data
		   
		    String path = "/user/ambari/AVG_TFIDF_matrix_words.csv";
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
		    
		    corpus.cache();
		    
		    /*
		     * ���ò���
		     * 
		     * 
		     * */
		    int dict_num = 300;//�ֵ�Ĵ�С
		    int item_num = 15;//ȡǰnum���������Ĵ�д���ļ���
		    int topic_num = 20;//���������
		    int MaxIterations = 100;//������������Ĭ��Ϊ20,һ��1000-2000����(���Ƕ�ջ����ˣ���)
		    int CheckpointInterval = 10;//�������Ĭ��Ϊ10
		    double Alpha = 1.2;//���ó����� alpha,һ������Ϊ 50/k + 1,�������1�����µĶ�������ľۼ��̶�DocumentConcentration 
		    double Beta = 1.1;//���ó�����beta Ĭ�� 1+0.	1,�������1 ������Դʵľۼ��̶�TopicConcentration
		    String Optimizer = "em";//ѡ���Ż���
		    
		    //���LDA��Ϣ
		    String LDA_info = "LDA ģ�Ͳ�����Ϣ\n"
		    				 +"�ֵ��С��"+Integer.toString(dict_num)+"\n"
		    				 +"ȡǰnum���������Ĵ�д���ļ���:"+Integer.toString(item_num)+"\n"
		    				 +"��������:"+Integer.toString(MaxIterations)+"\n"
		    				 +"���������:"+Integer.toString(CheckpointInterval)+"\n"
		    				 +"���������:"+Integer.toString(topic_num)+"\n"
		    				 +"���µĶ�������ľۼ��̶�DocumentConcentration:"+Double.toString(Alpha)+"\n"
		    				 +"����Դʵľۼ��̶�TopicConcentration:"+Double.toString(Beta)+"\n"
		    				 +"ѡ���Ż���:"+Optimizer+"\n\n";
		    
		    // Cluster the documents into three topics using LDA
		    LDAModel ldaModel = new LDA()
		    							 .setAlpha(Alpha)
		    							 .setBeta(Beta)
		    							 .setK(topic_num)
		    							 .setMaxIterations(MaxIterations)
		    							 .setCheckpointInterval(CheckpointInterval)
		    							 .setOptimizer(Optimizer)
		    							 .run(corpus);

		    
		    
		    //���ֵ��ļ� 
		    String words_path = "/user/ambari/cut_AVG_TFIDF_dict.csv";
		    
		    JavaRDD<String> words = jsc.textFile(words_path);
		    
		    List<String> wordsArr = words.collect();
		   
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
		    
		    //��ȫ������д�뵽HDFS�ļ�
		    List<String> all_topic_words = new ArrayList<>();
		    all_topic_words.add(LDA_info);
		    for(int i=0;i<topic_num;i++){
		    	String line = "���� "+Integer.toString(i+1)+" :";
		    	for(int j=0;j<dict_num;j++){
		    		line += Double.toString(out_matrix[i][j])+" ["+wordsArr.get(index[i][j])+"] ";
		    	}
		    	line += "\n";
		    	all_topic_words.add(line);
		    }
		    JavaRDD<String> all_topic_words_RDD = jsc.parallelize(all_topic_words);
		    all_topic_words_RDD.saveAsTextFile("/user/ambari/all_topic_words");
		    
		  //������item_num�������ʵľ���д�뵽HDFS�ļ�
		    List<String> num_topic_words = new ArrayList<>();
		    num_topic_words.add(LDA_info);
		    for(int i=0;i<topic_num;i++){
		    	String line = "���� "+Integer.toString(i+1)+" :";
		    	for(int j=0;j<item_num;j++){
		    		line += Double.toString(out_matrix[i][j])+" ["+wordsArr.get(index[i][j])+"] ";
		    	}
		    	line += "\n";
		    	num_topic_words.add(line);
		    }
		    JavaRDD<String> num_topic_words_RDD = jsc.parallelize(num_topic_words);
		    num_topic_words_RDD.saveAsTextFile("/user/ambari/part_topic_words");
		
		    //��ʾ�����Ϣ
		    System.out.println("finished!");
		    
		    jsc.stop();
		  }

}
