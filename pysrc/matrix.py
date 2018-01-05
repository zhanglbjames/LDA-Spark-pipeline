#!/usr/bin/python
#coding:utf-8
import codecs
import sys
import os
sys.path.append("../") 
'''
@author zss
生成词频矩阵
'''
def matrix():


	'''
	输入的文件：split-word-out.csv hundred_AVG_TFIDF_dict.csv out_all_words.csv
	'''
	
	#分词文件
	split_word_file = codecs.open("../split-word-out.csv","r","utf-8")
	split_word_lines = split_word_file.readlines()

	#stop-start个动词和名词的全局词典文件
	
	cut_dict_file = codecs.open("../cut_AVG_TFIDF_dict.csv","r","utf-8")
	cut_word_lines = cut_dict_file.readlines()
	'''
	输出的文件：AVG_TFIDF_matrix_words.csv
	'''
	#词频矩阵文件
	matrix_words_filename = "../AVG_TFIDF_matrix_words.csv"
	#判断文件是否存在，存在则删除
	if os.path.exists(matrix_words_filename):
		os.remove(matrix_words_filename)
	matrix_words_file = codecs.open(matrix_words_filename,"a+","utf-8")
	
	
	cut_words_list = []
	#将词典读出
	for word in cut_word_lines:
		cut_words_list.append(word)

	cut_dict_len = len(cut_words_list)
	#统计词频
	for line in split_word_lines:

		#初始化词频向量
		word_vector = [0 for i in range(cut_dict_len)]

		words_of_line = line.split(" ")
		for word in words_of_line:
			for i in range(cut_dict_len):
				if word == cut_words_list[i]:
					word_vector[i] += 1
			
		str_generator = (str(i) for i in word_vector )#构造生成器，用来生成字符向量
		str_vector = " ".join(str_generator)
		matrix_words_file.write(str_vector+"\n")
	

	#关闭文件流
	split_word_file.close()
	matrix_words_file.close()
	cut_dict_file.close()
	print "matrix finished"

if __name__ == '__main__':
	matrix()
