# =====================================================
# ================== GO LANG REBUILD ==================
# =====================================================
# Rebuild the kafka_exporter (which strimzi-kafka use) in order to fix `stdlib` vulnerability
FROM golang:1.23.3 AS golang-builder

# Pull the offical repository
RUN git clone https://github.com/danielqsj/kafka_exporter

# Change current working directory
WORKDIR /go/kafka_exporter

# Rebuild kafka_exporter with latest golang version
RUN make

# =========================================================================================
# ================== ICEBERG-AWS-BUNGLE and ICEBERG-AZURE-BUNDLE REBUILD ==================
# =========================================================================================
# Rebuild the `iceberg-aws-bundle` and `iceberg-azure-bundle` bundles with latest `netty-common` (4.1.115.Final)
FROM amazoncorretto:11-al2023-jdk AS iceberg-bundle-builder

# Arguments
ARG ICEBERG_VERSION=1.5.2
ARG NIMBUS_JOSE_JWT_VERSION=9.37.2
ARG NETTY_VERSION=4.1.115.Final

# Install git and pull the TAG
RUN dnf install git findutils -y

# Pull the offical iceberg-repository and update the packages for `iceberg-aws-bundle` and `iceberg-azure-bundle`
RUN git clone --branch apache-iceberg-${ICEBERG_VERSION} https://github.com/apache/iceberg.git

# Change workdir
WORKDIR /iceberg

# Inject force `io.netty` to be `4.1.115.Final`
RUN sed -i "25i\  configurations.all{resolutionStrategy.eachDependency{details->if(details.requested.group == 'io.netty'){details.useVersion '$NETTY_VERSION'}}}" aws-bundle/build.gradle \
    && sed -i "25i\  configurations.all{\nresolutionStrategy.force 'com.nimbusds:nimbus-jose-jwt:$NIMBUS_JOSE_JWT_VERSION'\nresolutionStrategy.force 'io.netty:netty-common:$NETTY_VERSION'}" azure-bundle/build.gradle

# Rebuild budles
RUN ./gradlew :iceberg-aws-bundle:clean :iceberg-aws-bundle:build :iceberg-azure-bundle:clean :iceberg-azure-bundle:build

# =====================================================================
# ================== ADDING PLUGINS TO STRIMZI/KAFKA ==================
# =====================================================================
# Build final strimzi/kafka image with extending it with iceberg-kafka-connect plugin (https://strimzi.io/docs/operators/latest/deploying#creating-new-image-from-base-str)
FROM quay.io/strimzi/kafka:0.44.0-kafka-3.8.0-amd64 AS base

# Setting up user to root to allow modifications
USER root

# Arguments
ARG PLUGIN_DISTRIBUTION_PACKAGE_URL="https://github.com/rapid7/iceberg-kafka-connect/releases/download/txid-rollver/iceberg-kafka-connect-runtime-txid.validity.rollover.v11.zip"
ARG LIBRARY_SCRIPT_NAME="replace_cc_kafka_libs.sh"
ARG ICEBERG_VERSION=1.5.2

# Update vulnerable krb5-libs and install wget to download files
RUN microdnf --setopt=install_weak_deps=0 --setopt=tsflags=nodocs install -y wget krb5-libs pam \
    && microdnf clean all -y

# Remove vulnerable dependencies using script, and replace them with fixed versions (with replaceing new version .jar filename to old .jar filename)
COPY ./${LIBRARY_SCRIPT_NAME} .
RUN ./${LIBRARY_SCRIPT_NAME} \
    && rm ${LIBRARY_SCRIPT_NAME}

# Copy custom plugin
RUN wget -O /opt/kafka/plugins/iceberg-kafka-connect.zip ${PLUGIN_DISTRIBUTION_PACKAGE_URL}
RUN unzip /opt/kafka/plugins/*.zip -d /opt/kafka/plugins \
    && rm /opt/kafka/plugins/*.zip \
    && for directory in /opt/kafka/plugins/iceberg-kafka-connect*; do mv "$directory" "/opt/kafka/plugins/iceberg-kafka-connect"; done

# Kafka exporter (just replace newly built golang binary from first stage with the one from image)
COPY --from=golang-builder /go/kafka_exporter/kafka_exporter /opt/kafka-exporter

# Replace bundles
COPY --from=iceberg-bundle-builder /iceberg/aws-bundle/build/libs/iceberg-aws-bundle-1.6.0-SNAPSHOT.jar /opt/kafka/plugins/iceberg-kafka-connect/lib/iceberg-aws-bundle-${ICEBERG_VERSION}.jar
COPY --from=iceberg-bundle-builder /iceberg/azure-bundle/build/libs/iceberg-azure-bundle-1.6.0-SNAPSHOT.jar /opt/kafka/plugins/iceberg-kafka-connect/lib/iceberg-azure-bundle-${ICEBERG_VERSION}.jar

# Set user back to 1001 (non-root)
USER 1001
