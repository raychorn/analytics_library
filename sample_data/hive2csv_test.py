#!/usr/bin/env python
import sys

line = "SMSIQLMGSMWIFI\t17789975733\t\"['ENU', 'CHS', 'CHT', 'DAN', 'DEU', 'FRA', 'JPN', 'KOR', 'THA']\"\tENU\n"
result = []
row = line.rstrip('\n').split('\t')
for column in row:
	if not column.isdigit() and (not column.startswith('"[') and not column.endswith('"]')):
		result.append("\"" + column + "\"")
	else:
		result.append(column)
print ",".join(result)