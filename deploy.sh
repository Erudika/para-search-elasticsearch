#!/bin/bash
echo "Listing all tags:"
git tag
echo "---"
read -e -p "Version tag: " ver
read -e -p "Last version tag: " lastver
read -e -p "New dev version: " devver

git add -A && git commit -m "Release v$ver." && git push origin master && \
mvn --batch-mode -Dtag=${ver} release:prepare -DreleaseVersion=${ver} -DdevelopmentVersion=${devver}-SNAPSHOT && \
mvn release:perform && \
echo "Maven release done, publishing release on GitHub..," && \
echo "v$ver" > changelog.txt && \
echo "" >> changelog.txt && \
git log $lastver..HEAD --oneline >> changelog.txt && \
echo "" >> changelog.txt && \
echo "" >> changelog.txt && \
echo "### :package: [Download JAR](https://oss.sonatype.org/service/local/repositories/releases/content/com/erudika/para-search-elasticsearch/${ver}/para-search-elasticsearch-${ver}-shaded.jar)" >> changelog.txt && \
hub release create -F changelog.txt $ver && \
rm changelog.txt