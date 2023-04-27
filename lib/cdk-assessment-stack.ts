import { Duration, Stack, StackProps, RemovalPolicy } from 'aws-cdk-lib';
import * as sns from 'aws-cdk-lib/aws-sns';
import * as subs from 'aws-cdk-lib/aws-sns-subscriptions';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import { AttributeType, Table } from 'aws-cdk-lib/aws-dynamodb';
import { Function, Runtime, AssetCode } from "aws-cdk-lib/aws-lambda"
import { Construct } from 'constructs';

export class CdkAssessmentStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    const queue = new sqs.Queue(this, 'CdkAssessmentQueue', {
      visibilityTimeout: Duration.seconds(300)
    });

    const topic = new sns.Topic(this, 'CdkAssessmentTopic');

    topic.addSubscription(new subs.SqsSubscription(queue));

    const tableName = 'userTable';
    const dynamoTable = new Table(this, tableName, {
      partitionKey: {
        name: 'id',
        type: AttributeType.STRING
      },
      tableName: tableName,
      removalPolicy: RemovalPolicy.DESTROY, // NOT recommended for production code
    });

    const userStreamProcessorLambda = new Function(this, 'UserStreamProcessor', {
      functionName: 'UserStreamProcessor',
      handler: "handler.handler",
      runtime: Runtime.NODEJS_16_X,
      code: new AssetCode(`./src`),
      memorySize: 512,
      timeout: Duration.seconds(10),
      environment: {
        TABLE_NAME: dynamoTable.tableName,
        IDENTITY_LOOKUP_QUEUE: queue.queueUrl
      }
    });

    dynamoTable.grantReadWriteData(userStreamProcessorLambda);
    queue.grantSendMessages(userStreamProcessorLambda);
  }
}
