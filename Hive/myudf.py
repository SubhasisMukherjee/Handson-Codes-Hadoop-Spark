import sys
import datetime

for line in sys.stdin:
    line = line.strip()
    fname , lname = line.split('\t')
    l_name = lname.lower()
    print '\t'.join([fname, str(l_name)])
