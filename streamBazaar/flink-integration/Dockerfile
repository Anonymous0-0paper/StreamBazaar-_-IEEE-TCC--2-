FROM maven:3.9.0-eclipse-temurin-11 as builder

WORKDIR /app

# Copy pom and sources
COPY flink-integration/pom.xml ./pom.xml
COPY flink-integration/src ./src

# Build the project
RUN mvn clean package -DskipTests

# Runtime image: use Flink base image
FROM flink:1.18.0-scala_2.12-java11

# Copy built JAR to Flink lib directory
COPY --from=builder /app/target/flink-integration.jar /opt/flink/lib/flink-integration.jar

# Copy log4j configuration
COPY configs/log4j.properties /opt/flink/conf/log4j.properties

WORKDIR /opt/flink
