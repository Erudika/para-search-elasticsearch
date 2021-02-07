FROM openjdk:11-jre-slim

RUN mkdir -p /para/lib

WORKDIR /para

ENV PARA_PLUGIN_ID="para-search-elasticsearch" \
	PARA_PLUGIN_VER="1.36.0"

ADD https://oss.sonatype.org/service/local/repositories/releases/content/com/erudika/$PARA_PLUGIN_ID/$PARA_PLUGIN_VER/$PARA_PLUGIN_ID-$PARA_PLUGIN_VER-shaded.jar /para/lib/
