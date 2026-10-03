# Backend image. The jar is built by `./gradlew bootJar` (see platform-k8s/event-console/setup.sh),
# so the image contains only a JRE and the app -- no build tools, no source.
FROM eclipse-temurin:26-jre-alpine

# Numeric UID so Kubernetes' runAsNonRoot can verify it without resolving a name.
RUN addgroup -S -g 10001 app && adduser -S -u 10001 -G app app
WORKDIR /app
COPY build/libs/event-console-consumer.jar app.jar
USER 10001:10001

EXPOSE 8080
# Size the heap from the container's memory limit, and die on OOM so Kubernetes
# restarts the pod instead of leaving a half-alive JVM running.
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=70 -XX:+ExitOnOutOfMemoryError"
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
