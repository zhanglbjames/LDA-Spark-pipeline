#!/usr/bin/python
#coding:utf-8
import codecs
import sys
import os
import math
import jieba.posseg as pseg
sys.path.append("../") 
'''
@author zss
生成排名前100个名词的全局词典,其中计算排名的数值采用均值全局字典TFIDF模型 ，公式如下：(n/w)*log(N/F)
其中，n表示词在整个语料中总的出现次数（含重复），w表示整个语料中总的词数，N表示语料的个数，F表示包含某个词的预料的数量
'''

def TFIDF_dict(start=0,stop=100):
	'''
	输入的文件：split-word-out.csv sorted_dict.csv out_all_words.csv
	'''

	#分词文件
	split_word_file = codecs.open("../split-word-out.csv","r","utf-8")
	split_word_lines = split_word_file.readlines()
	
	#词频文件
	sorted_dict_file = codecs.open("../sorted_dict.csv","r","utf-8")
	sorted_dict_lines = sorted_dict_file.readlines()

	'''
	输出文件： AVG_TFIDF_dict.csv  hundred_AVG_TFIDF_dict.csv
	'''
	#名词的全局词典文件
	AVG_TFIDF_dict_filename = "../AVG_TFIDF_dict.csv"
	#判断文件是否存在，存在则删除
	if os.path.exists(AVG_TFIDF_dict_filename):
		os.remove(AVG_TFIDF_dict_filename)
	AVG_TFIDF_dict_file = codecs.open(AVG_TFIDF_dict_filename,"a+","utf-8")


	#stop-start个动名词和名词的全局词典文件
	cut_AVG_TFIDF_dict_filename = "../cut_AVG_TFIDF_dict.csv"
	#判断文件是否存在，存在则删除
	if os.path.exists(cut_AVG_TFIDF_dict_filename):
		os.remove(cut_AVG_TFIDF_dict_filename)
	cut_AVG_TFIDF_dict_file = codecs.open(cut_AVG_TFIDF_dict_filename,"a+","utf-8")

	
	#统计文档的总数，以及每行的词-set
	NUM = 1.0 #初始化为浮点数
	lines_list = []
	for line in split_word_lines:
		NUM += 1
		lines_list.append(set(line.split(" ")))


	#将排序后的词取出
	v_n_list = []
	for line in sorted_dict_lines:
		kv_word = line.split(" ")
		v_n_list.append(kv_word[0])

	#计算包含每一个词文档的数量
	vector_list = {word:1 for word in v_n_list}#初始化IDF，key为词，value IDF

	for line1 in lines_list:
		for word1 in line1:
			if word1 in vector_list:
				for line2 in lines_list:
					if word1 in line2:
						vector_list[word1] += 1


	#均值全局字典TFIDF
	kv_all_words_dict = {}

	for line in sorted_dict_lines:
		kv_word = line.split(" ")
		kv_all_words_dict[kv_word[0]] = int(kv_word[1])*math.log(NUM/vector_list[kv_word[0]])
		

	#对字典按照词频由高到低排序，iteritems()表示的是迭代出k,v  lambda表达式，输入（ector_list.iteritems()） ：输出为v,[kv01]
	sort_TFIDF_tuple = sorted(kv_all_words_dict.iteritems(),key=lambda t:t[1],reverse=True)
	sort_TFIDF_list = [k+" "+str(v) for k,v in sort_TFIDF_tuple]
	AVG_TFIDF_dict_file.write("\n".join(sort_TFIDF_list))#将排序的词频写入

	cut_TFIDF_dict = [k for k,v in sort_TFIDF_tuple[start:stop]] #构造排名从start到stop的的列表
	cut_AVG_TFIDF_dict_file.write("\n".join(cut_TFIDF_dict))

	#关闭文件流
	split_word_file.close()
	sorted_dict_file.close()
	cut_AVG_TFIDF_dict_file.close()
	AVG_TFIDF_dict_file.close()
	
	print "TFIDF dict finishes!"

if __name__ == '__main__':
	TFIDF_dict()
