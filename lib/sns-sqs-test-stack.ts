import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as sns from 'aws-cdk-lib/aws-sns';
import * as subscriptions from 'aws-cdk-lib/aws-sns-subscriptions';

export class SnsSqsTestStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const topic = new sns.Topic(this, 'payload-key-topic', {
      topicName: 'payload-key-topic',
    });

    const queueV1 = new sqs.Queue(this, 'payload-v1-key-queue', {
      queueName: 'payload-v1-key-queue',
    });

    const queueV2 = new sqs.Queue(this, 'payload-v2-key-queue', {
      queueName: 'payload-v2-key-queue',
    });

    topic.addSubscription(new subscriptions.SqsSubscription(queueV1, {
      filterPolicy: {
        version: sns.SubscriptionFilter.numericFilter({
          allowlist: [1],
        }),
      },
      rawMessageDelivery: true,
    }));

    topic.addSubscription(new subscriptions.SqsSubscription(queueV2, {
      filterPolicy: {
        version: sns.SubscriptionFilter.numericFilter({
          allowlist: [2],
        }),
      },
      rawMessageDelivery: true,
    }));
  }
}
