#!/usr/bin/python -u
import time
import random
import json
import os
import sys
# reopen stdout file descriptor with write mode
# and 0 as the buffer size (unbuffered)
# sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)
def main():
	try:
		status_codes = [200, 200, 200, 200, 200, 200, 500]
		urls = ['/login', '/posts', '/submit', '/']
		total_outputs = 0
		while True:
			url = random.choice(urls)
			status_code = random.choice(status_codes)
			data = dict(status_code=status_code, response_ms=random.randint(1, len(url)*5), url=url)			    
			sys.stdout.write(json.dumps(data) + '\n')
			total_outputs += 1
			time.sleep(random.randint(1,50) / 1000.0)
	except KeyboardInterrupt:
		sys.stderr.write("Total rows: {}".format(total_outputs))
if __name__ == "__main__":
	main()
