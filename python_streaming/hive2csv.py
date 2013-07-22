#!/usr/bin/env python
import sys

for line in sys.stdin:
	result = []
	row = line.rstrip('\n').split('\t')
	for column in row:
		if not column.isdigit() and (not column.startswith('"[') and not column.endswith('"]')):
			result.append("\"" + column + "\"")
		else:
			result.append(column)
	print ",".join(result)