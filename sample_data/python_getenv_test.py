#!/usr/bin/env python
import sys, os

print str(os.getenv("ANALYTICS_LIB"))

def main(argv=None):
  if argv is None:
    argv = sys.argv
  
  FILENAME='file.txt'
  FILESIZE='23455'
  print "'File:%s,Size:%s'" % (FILENAME,FILESIZE) 
  TEXT = "Message:\n"
  for item in (argv[1].split('\\n')):
    TEXT = TEXT + item + '\n'
  print TEXT

  print "text\ntext"
  print argv[1]


if __name__ == "__main__":
  sys.exit(main())
