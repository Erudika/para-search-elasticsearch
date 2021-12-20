FROM adoptopenjdk/openjdk11:ubi-minimal-jre

RUN mkdir -p /para/lib

WORKDIR /para

ENV PARA_PLUGIN_ID="para-search-elasticsearch" \
	PARA_PLUGIN_VER="1.38.0"

ADD https://oss.sonatype.org/service/local/repositories/releases/content/com/erudika/$PARA_PLUGIN_ID/$PARA_PLUGIN_VER/$PARA_PLUGIN_ID-$PARA_PLUGIN_VER-shaded.jar /para/lib/
