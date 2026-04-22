FROM maven:3.9.0-eclipse-temurin-11 as builder

WORKDIR /build

# Copy the flink-integration project (context is root directory)
COPY flink-integration/ ./

# Build the StreamBazaar Flink integration JAR
RUN mvn clean package -DskipTests

# Runtime stage: use standard Flink image
FROM flink:1.18-scala_2.12-java11

# Copy the built StreamBazaar JAR to Flink lib
COPY --from=builder /build/target/flink-integration.jar /opt/flink/lib/
