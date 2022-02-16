#!/bin/bash
lastver=$(git describe --abbrev=0 --tags)
echo "Last tag was: $lastver"
echo "---"
read -e -p "New version: " ver
read -e -p "New dev version: " devver

sed -i -e "s/PARA_PLUGIN_VER=.*/PARA_PLUGIN_VER="\"$ver\""/g" Dockerfile && \

git add -A && git commit -m "Release v$ver." && git push origin master && \
mvn --batch-mode -Dtag=${ver} release:prepare -Dresume=false -Dresume=false -DreleaseVersion=${ver} -DdevelopmentVersion=${devver}-SNAPSHOT && \
mvn release:perform && \
echo "Maven release done, publishing release on GitHub..," && \
git log $lastver..HEAD --oneline > changelog.txt && \
echo "" >> changelog.txt && \
echo "" >> changelog.txt && \
echo "### :package: [Download JAR](https://oss.sonatype.org/service/local/repositories/releases/content/com/erudika/para-search-elasticsearch/${ver}/para-search-elasticsearch-${ver}-shaded.jar)" >> changelog.txt && \
hub release create -F changelog.txt -t "$ver" $ver && \
rm changelog.txt

docker build -t erudikaltd/para-search-elasticsearch:${ver} . && docker push erudikaltd/para-search-elasticsearch:${ver}
