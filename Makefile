PYTHON=/usr/bin/python
PYTHONPATH=.

test:
	PYTHONPATH=${PYTHONPATH} ${PYTHON} tests/basic.py