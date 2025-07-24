# ---------- STAGE 1: Build with Maven ----------
FROM maven:3.9.6-eclipse-temurin-17 AS builder

WORKDIR /build
COPY . .

# Build the fat JAR (skip tests for CI/CD speed, or remove -DskipTests if needed)
RUN mvn clean package -DskipTests

# ---------- STAGE 2: Runtime with Flink ----------
FROM flink:1.20.0-scala_2.12-java17

# Set working dir and copy only the fat JAR from the builder
WORKDIR /opt/flink/usrlib
COPY --from=builder /build/target/flink-poc-*.jar ./flink-poc.jar

# Optional: copy custom Flink config or logging if needed
# COPY conf/flink-conf.yaml /opt/flink/conf/

# The actual command is handled by Flink Helm chart or entrypoint
# Example args in Helm:
#   - standalone-job
#   - --job-classname
#   - com.sage.flink.FlinkJob
#   - /opt/flink/usrlib/flink-poc.jar
