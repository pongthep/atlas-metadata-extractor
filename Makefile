clean_dist:
	rm -rf ./dist/*

build:
	python3 setup.py sdist

upload:
	twine upload -r pypi dist/*

release: clean_dist build upload