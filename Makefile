install:
	pip3 install -r requirements.txt

run:
	python3 main.py

run-drop-db:
	rm genes.db; python3 main.py

