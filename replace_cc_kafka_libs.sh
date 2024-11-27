#!/bin/bash
# Declaration of 3rd party libraries to replace (key - .jar file of vulnerable dependency, value - link to maven repo to .jar file of newest version)
declare -A cruise_control_libraries=(
 ["commons-io-2.11.0.jar"]="https://repo1.maven.org/maven2/commons-io/commons-io/2.18.0/commons-io-2.18.0.jar"
 ["nimbus-jose-jwt-9.24.jar"]="https://repo1.maven.org/maven2/com/nimbusds/nimbus-jose-jwt/9.37.2/nimbus-jose-jwt-9.37.2.jar"
 ["netty-common-4.1.110.Final.jar"]="https://repo1.maven.org/maven2/io/netty/netty-common/4.1.115.Final/netty-common-4.1.115.Final.jar"
 ["kafka-metadata-3.5.1.jar"]="https://repo1.maven.org/maven2/org/apache/kafka/kafka-metadata/3.6.2/kafka-metadata-3.6.2.jar"
)
declare -A kafka_libraries=(
 ["commons-io-2.11.0.jar"]="https://repo1.maven.org/maven2/commons-io/commons-io/2.18.0/commons-io-2.18.0.jar"
 ["protobuf-java-3.23.4.jar"]="https://repo1.maven.org/maven2/com/google/protobuf/protobuf-java/3.25.5/protobuf-java-3.25.5.jar"
 ["netty-common-4.1.110.Final.jar"]="https://repo1.maven.org/maven2/io/netty/netty-common/4.1.115.Final/netty-common-4.1.115.Final.jar"
)

# CRUISE CONTROL - Iterate, download and replace .jars
for key in "${!cruise_control_libraries[@]}"; do
    rm /opt/cruise_control/libs/$key
    wget -O /opt/cruise-control/libs/$key ${cruise_control_libraries[$key]}
done

# KAFKA - Iterate, download and replace .jars
for key in "${!kafka_libraries[@]}"; do
    rm /opt/kafka/libs/$key
    wget -O /opt/kafka/libs/$key ${kafka_libraries[$key]}
done
