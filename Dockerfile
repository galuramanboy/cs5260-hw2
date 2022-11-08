FROM openjdk:16
COPY Assn2.jar Consumer.jar
CMD ["java", "-jar", "Consumer.jar", "-rq", "https://sqs.us-east-1.amazonaws.com/905808720665/cs5260-requests", "-wtb", "widgets"]