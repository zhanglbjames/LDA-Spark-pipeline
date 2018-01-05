#!/usr/bin/python
#coding:utf-8
import codecs
import sys
import os 
import jieba.posseg as pseg
sys.path.append("../") 
'''
@author zss
生成排名前100个名词的全局词典
'''
def sorted_dict():

	'''
	输入：分词文件，整个语料库中所有的不重复的词文件
	'''
	split_word_file = codecs.open("../split-word-out.csv","r","utf-8")
	split_word_lines = split_word_file.readlines()

	all_words_file = codecs.open("../out_all_words.csv","r","utf-8")
	all_word_lines = all_words_file.readlines()

	'''
	输出为：名词的全局排序词频文件
	'''
	
	sorted_dict_filename = "../sorted_dict.csv"
	#判断文件是否存在，存在则删除
	if os.path.exists(sorted_dict_filename):
		os.remove(sorted_dict_filename)
	sorted_dict_file = codecs.open(sorted_dict_filename,"a+","utf-8")

	
	v_n_list = []
	#将语料库中的词只保留名词
	for line in all_word_lines:
		a_posseg = pseg.cut(line)
		for word,flag in a_posseg:
			if flag == "n" or flag == "ni" or flag == "nz":
				v_n_list.append(word)

	vector_list = {word:0 for word in v_n_list}#初始化统计词频向量字典，key为词，value为词频

	#统计词频
	for line in split_word_lines:
		words_of_line = line.split(" ")
		for word in words_of_line:
			for v_n_key,v_n_value in vector_list.items():
				if word == v_n_key:
					vector_list[v_n_key] += 1

	#对字典按照词频由高到低排序，iteritems()表示的是迭代出k,v  lambda表达式，输入（ector_list.iteritems()） ：输出为v,[kv01]
	sort_vector_tuple = sorted(vector_list.iteritems(),key=lambda t:t[1],reverse=True)
	sort_vector_list = [k+" "+str(v) for k,v in sort_vector_tuple]
	sorted_dict_file.write("\n".join(sort_vector_list))#将排序的词频写入

	hundred_dict = [k for k,v in sort_vector_tuple[:100]] #构造只含有词的排名前一百的列表
	

	#关闭文件流
	split_word_file.close()
	all_words_file.close()
	sorted_dict_file.close()
	
	print "sorted dict finishes!"

if __name__ == '__main__':
	sorted_dict()
