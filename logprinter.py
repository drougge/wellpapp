#!/usr/bin/env python

import sys

tags = {}

def TP(words):
	line = words[0]
	for tag in words[1:]:
		name = tag[1:]
		try:
			name = tags[name]
			line += " N" + name
		except:
			line += " G" + name
	return line

def AT(words):
	assert words[0][0] == "G"
	guid = words[0][1:]
	for w in words:
		if w[0] == "N": tags[guid] = w[1:]

for fn in sys.argv[1:]:
	for line in file(fn):
		line = line[:-1]
		if line[0] == 'D':
			act = line[18:20]
			rest = line[20:].split()
			if act == "TP":
				line = line[:20] + TP(rest)
			elif act == "AT":
				AT(rest)
			elif act == "MT":
				raise "MT not handled yet"
		print line
