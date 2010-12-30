#!/usr/bin/env python
# -*- coding: iso-8859-1 -*-

import struct

charset = "abcdefghkopqrstyABCDEFGHKLPQRSTY234567890"
base = 41

data = file("/dev/random").read(7) + "\0\0\0\0\0\0\0\0"
sum = 0;
for c in data: sum += ord(c)
data = data[:7] + chr(sum % 256) + data[7:]

guid = ""
for val in struct.unpack(">IIII", data):
	fmt = "-"
	for i in range(0, 6):
		v = val % base
		val //= base
		fmt = charset[v] + fmt
	guid = guid + fmt
print guid[:-1]
