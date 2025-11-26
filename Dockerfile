FROM alpine

ENV PARA_PLUGIN_ID="para-search-elasticsearch" \
	PARA_PLUGIN_VER="1.42.0"

ADD https://repo1.maven.org/maven2/com/erudika/$PARA_PLUGIN_ID/$PARA_PLUGIN_VER/$PARA_PLUGIN_ID-$PARA_PLUGIN_VER-shaded.jar /para/lib/
