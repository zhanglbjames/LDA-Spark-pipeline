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

    SparkConf conf = new SparkConf().setAppName("mllda");//.setMaster("local");//设置单机运行
    JavaSparkContext jsc = new JavaSparkContext(conf);

    // $example on$
    // Load and parse the data
    String path = "file:///home/jht/spark-input-data/AVG_TFIDF_matrix_words.csv";
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
     * 配置参数
     * 
     * 
     * */
    int dict_num = 300;//字典的大小
    int item_num = 15;//取前num个概率最大的词写到文件中
    int topic_num = 20;//主题的数量
    int MaxIterations = 100;//最大迭代次数，默认为20,一般1000-2000收敛(但是堆栈溢出了，囧)
    int CheckpointInterval = 10;//检查间隔。默认为10
    double Alpha = 1.2;//设置超参数 alpha,一般设置为 50/k + 1,必须大于1，文章的对于主题的聚集程度DocumentConcentration 
    double Beta = 1.1;//设置超参数beta 默认 1+0.	1,必须大于1 ，主题对词的聚集程度TopicConcentration
    String Optimizer = "em";//选择优化器
    
    //输出LDA信息
    String LDA_info = "LDA 模型参数信息\n"
    				 +"字典大小："+Integer.toString(dict_num)+"\n"
    				 +"取前num个概率最大的词写到文件中:"+Integer.toString(item_num)+"\n"
    				 +"迭代次数:"+Integer.toString(MaxIterations)+"\n"
    				 +"检查迭代间隔:"+Integer.toString(CheckpointInterval)+"\n"
    				 +"主题的数量:"+Integer.toString(topic_num)+"\n"
    				 +"文章的对于主题的聚集程度DocumentConcentration:"+Double.toString(Alpha)+"\n"
    				 +"主题对词的聚集程度TopicConcentration:"+Double.toString(Beta)+"\n"
    				 +"选择优化器:"+Optimizer+"\n\n";
    
    // Cluster the documents into three topics using LDA
    LDAModel ldaModel = new LDA()
    							 .setAlpha(Alpha)
    							 .setBeta(Beta)
    							 .setK(topic_num)
    							 .setMaxIterations(MaxIterations)
    							 .setCheckpointInterval(CheckpointInterval)
    							 .setOptimizer(Optimizer)
    							 .run(corpus);

    
    
    //打开字典文件
    File cutWords = new File("/home/jht/spark-input-data/cut_AVG_TFIDF_dict.csv");
    BufferedReader br=new BufferedReader(new InputStreamReader(new FileInputStream(cutWords),"utf-8"));
    
    String[] wordsArr = new String[dict_num];
    
    //将词典读出到数组中
    for(int i=0;i<dict_num;i++){
    	wordsArr[i] = br.readLine();
    }
    //全部排序的文件
    File topicFileAllWords = new File("/home/jht/spark-out-data/topic_matrix_all_words.csv");
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
    bw.write(LDA_info);
    
  //item_num个最大概率的词排序的文件
    File hundredtopicFileAllWords = new File("/home/jht/spark-out-data/topic_matrix_num_words.csv");
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
    
    h_bw.write(LDA_info);
    
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
    		line += Double.toString(out_matrix[i][j])+" ["+wordsArr[index[i][j]]+"] ";
    	}
    	line += "\n";
    	bw.write(line);
    }
  //将包含item_num个最大概率的矩阵写入到文件
    for(int i=0;i<topic_num;i++){
    	String line = "主题 "+Integer.toString(i+1)+" :";
    	for(int j=0;j<item_num;j++){
    		line += Double.toString(out_matrix[i][j])+" ["+wordsArr[index[i][j]]+"] ";
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
