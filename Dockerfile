FROM eclipse-temurin:17-jre
WORKDIR /app

COPY target/canal-web-0.1.0-SNAPSHOT.jar app.jar
COPY canal-runtime canal-runtime
COPY entrypoint.sh entrypoint.sh
RUN chmod +x entrypoint.sh canal-runtime/canal-server/bin/*.sh canal-runtime/canal-adapter/bin/*.sh

EXPOSE 18082 11111 11112 18083
ENTRYPOINT ["./entrypoint.sh"]
