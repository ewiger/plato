clean:
		rm -rf dist build *.egg-info
		find ./ -type d -name '*.egg-info' | xargs rm -rf

dist:
		python setup.py bdist_egg

