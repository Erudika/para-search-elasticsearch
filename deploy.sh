#!/bin/bash
read -e -p "Tag: " ver
sed -i -e "s/\"version\":.*/\"version\": "\"$ver\"",/g" package.json
git add -A && git commit -m "Release v$ver."
git tag "v$ver"
git push origin master && git push --tags
npm publish
