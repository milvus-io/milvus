How to release
--------------

1. Make sure you're on master. `git checkout master && git fetch && git reset --hard origin/master`
1. Update `setup.py` to the newest version, `git add setup.py && git commit -m "version 1.2.3"`
1. `python setup.py sdist bdist_wheel`
1. `git tag -a v1.2.3 -m "version 1.2.3"`
1. `git push --tags origin master` to push the last version to Github
1. Go to https://github.com/spotify/annoy/releases and click "Draft a new release"
1. `twine upload dist/annoy-1.2.3*`

TODO
----

* Wheel
