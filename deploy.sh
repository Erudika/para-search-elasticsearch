#!/bin/bash
echo "Run mvn release:perform first!"
read -e -p "Tag: " ver
read -e -p "Last tag: " lastver
git add -A && git commit -m "Release v$ver." && git push origin master
echo "v$ver" > changelog.txt
echo "" >> changelog.txt
git log $lastver..HEAD --oneline >> changelog.txt
echo "" >> changelog.txt
echo "" >> changelog.txt
echo "### :package: [Download JAR](https://oss.sonatype.org/service/local/repositories/releases/content/com/erudika/para-search-elasticsearch/${ver}/para-search-elasticsearch-${ver}-shaded.jar)" >> changelog.txt
hub release create -F changelog.txt $ver
cat changelog.txt
rm changelog.txt