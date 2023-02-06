build:
	rm -rf dist
	rm -rf Output
	mkdir app
	cp main.py ./app
	cp config.yaml ./app
	cp -r Data ./app
	cd src && zip -r ../app/src.zip .
