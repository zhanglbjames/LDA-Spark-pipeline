#!/usr/bin/python
#coding:utf-8
from word import word
from sorted_dict import sorted_dict
from TFIDF_dict import TFIDF_dict
from matrix import matrix

if __name__ == '__main__':
	word()
	sorted_dict()
	#指定词典的区域,词典大小为300个词
	TFIDF_dict(0,300)
	matrix()