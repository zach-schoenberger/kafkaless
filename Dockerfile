FROM openjdk:10-jre-slim

WORKDIR /
ADD build/distributions/kafkaless-0.0.1.tar .
WORKDIR /kafkaless-0.0.1/bin/
CMD /kafkaless-0.0.1/bin/kafkaless
