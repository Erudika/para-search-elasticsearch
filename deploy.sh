#!/bin/bash
read -e -p "Tag: " ver
git add -A && git commit -m "Release v$ver."
git tag "$ver"
git push origin master && git push --tags

echo "v$ver \n" > changelog.txt
git log $ver..HEAD --oneline >> changelog.txt
hub release create -F changelog.txt -a target/para-search-elasticsearch-*.jar 