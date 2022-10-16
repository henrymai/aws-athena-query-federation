FROM public.ecr.aws/lambda/java:11

ARG version

COPY target/athena-dynamodb-${version}.jar ${LAMBDA_TASK_ROOT}/lib/athena-dynamodb-${version}.jar

CMD [ "com.amazonaws.athena.connectors.dynamodb.DynamoDBCompositeHandler::handleRequest" ]
