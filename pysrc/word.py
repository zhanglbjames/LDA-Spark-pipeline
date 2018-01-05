#!/usr/bin/python
#coding:utf-8
import codecs
import sys
import os
import re
import jieba
sys.path.append("../") #将当前源码所在的目录的路径添加到path中
#jieba.load_userdict("userdict.txt")#添加自定义词典
'''
@author zss
分词，去停用词，生成全语料无重复词库
所有的输入文件，在输入之前需编码成utf-8
'''

def word():
	'''
	输入文件为：过滤噪声后的文件，中文停用词文件，英文停用词文件

	'''
	inputfile = codecs.open("../filter-out.csv","r","utf-8")
	list_lines = inputfile.readlines()

	c_stop_file = codecs.open("../stop-dict/chinese_stop.txt","r","utf-8")#中文停用词
	c_stops = c_stop_file.readlines()

	e_stop_file = codecs.open("../stop-dict/english_stop.txt","r","utf-8")#英文停用词
	e_stops = e_stop_file.readlines()

	c_stop_list = [stop.strip() for stop in c_stops ]
	e_stop_list = [stop.strip() for stop in e_stops ]

	c_stop_set = set(c_stop_list)#去重
	e_stop_set = set(e_stop_list)#去重

	'''
	输出文件为：原始分词后的数据，整个语料库中所有的不重复的词文件

	'''
	outputfilename = "../split-word-out.csv"
	#判断文件是否存在，存在则删除
	if os.path.exists(outputfilename):
		os.remove(outputfilename)
	outputfile = codecs.open(outputfilename,"a+","utf-8")#输出的词袋数据

	out_all_wordsname = "../out_all_words.csv"
	#判断文件是否存在，存在则删除
	if os.path.exists(out_all_wordsname):
		os.remove(out_all_wordsname)
	out_all_words = codecs.open(out_all_wordsname,"a+","utf-8")#输出整个语料库中所有的不重复的词

	all_words = set([])#所有不重复单词的结合。
	
	'''
	注意分词的时候换行符是被分为一个词的，所以后面写入的时候就不用再尾追换行符
	分词的时候连续多个空格被分为多个单个空格
	'''
	pattern = re.compile(r'^[a-zA-Z0-9]+$')#从头到尾全匹配纯字符和数字的组合


	for line in list_lines:
		seg_list = list(jieba.cut(line))
		newlist = []
		for seg in seg_list:
			#过滤掉停用词，空格,换行符，单个汉字词，一个汉字长度为1 ，纯字符和数字的组合
			match = pattern.match(seg)
			if seg not in c_stop_list and seg not in e_stop_list and seg != " " and seg != "\n" and len(seg) >= 2 and match == None:
				newlist.append(seg)

		text =  ' '.join(newlist)

		#过滤只含有空白符的行
		if(len(text.strip()) != 0):
			outputfile.write(text+"\n")
			
			list_set = set(newlist)

			all_words = set(list_set|all_words)#添加到全局词组
	
	#将全局词库，写到文件中
	out_all_words.write("\n".join(all_words))
	print len(all_words)

	outputfile.close()
	inputfile.close()
	c_stop_file.close()
	e_stop_file.close()
	out_all_words.close()

	print "word finished!"

if __name__ == '__main__':
	word()