import * as cdk from '@aws-cdk/core';
import { Stream } from '@aws-cdk/aws-kinesis';
import { Bucket } from '@aws-cdk/aws-s3';
import * as analytics from '@aws-cdk/aws-kinesisanalytics';
import { Role, ServicePrincipal, PolicyDocument, PolicyStatement, Effect } from '@aws-cdk/aws-iam'
import { LogGroup, LogStream, RetentionDays } from '@aws-cdk/aws-logs'
import S3 = require('aws-sdk/clients/s3')
import { createHash } from 'crypto'

export class CdkKdaPyFlinkSampleStack extends cdk.Stack {
  constructor(scope: cdk.Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);
    const artifactBucketArn = "arn:aws:s3:::hogefuga"
    const srcStream = this.srcStream()
    const distBucket = this.distBucket()
    const { logGroup, logStream } = this.kdaLog()
    const kdaRole = this.kdaRole(artifactBucketArn, srcStream, distBucket, logGroup, logStream)
    new S3().getObject({
      Bucket: "hogefuga",
      Key: "kda-app.zip"
    }, (err, data) => {
      if (err) throw err
      const appHash = createHash("sha1").update(data.Body!.toString()).digest("hex")
      console.log(`appHash: ${appHash}`)
      this.kdaApp(kdaRole, artifactBucketArn, srcStream, distBucket, logGroup, logStream, appHash)
    })
  }

  srcStream = () => {
    return new Stream(this, "SrcStream", {
      streamName: `cdk-kda-pyflink-sample-stream`,
      shardCount: 3,
    });
  }

  distBucket = () => {
    return new Bucket(this, "DistBucket", {
      bucketName: `cdk-kda-pyflink-sample-bucket-${this.account}`,
    });
  }

  kdaLog = () => {
    const logGroup = new LogGroup(this, 'KDALogGroup', {
      retention: RetentionDays.ONE_MONTH
    })
    const logStream = new LogStream(this, "KDALogStream", {
      logGroup: logGroup,
    })
    return { logGroup, logStream }
  }

  kdaRole = (artifactBucketArn: string, srcStream: Stream, destBucket: Bucket, logGroup: LogGroup, logStream: LogStream) => {
    return new Role(this, 'KDARole', {
      assumedBy: new ServicePrincipal('kinesisanalytics.amazonaws.com'),
      inlinePolicies: {
        'test-analytics-policy': new PolicyDocument({
          statements: [
            // https://docs.aws.amazon.com/kinesisanalytics/latest/dev/iam-role.html
            new PolicyStatement({
              effect: Effect.ALLOW,
              actions: [
                "kinesis:DescribeStreamSummary",
                "kinesis:GetShardIterator",
                "kinesis:GetRecords",
                "kinesis:ListShards",
              ],
              resources: [
                srcStream.streamArn
              ]
            }),
            // https://docs.aws.amazon.com/kinesisanalytics/latest/java/examples-s3.html
            new PolicyStatement({
              effect: Effect.ALLOW,
              actions: [
                "s3:Abort*",
                "s3:DeleteObject*",
                "s3:GetObject*",
                "s3:GetBucket*",
                "s3:List*",
                "s3:ListBucket",
                "s3:PutObject"
              ],
              resources: [
                destBucket.bucketArn,
                `${destBucket.bucketArn}/*`
              ]
            }),
            new PolicyStatement({
              effect: Effect.ALLOW,
              actions: [
                "logs:DescribeLogGroups",
              ],
              resources: [
                `arn:aws:logs:${this.region}:${this.account}:log-group:*`
              ]
            }),
            new PolicyStatement({
              effect: Effect.ALLOW,
              actions: [
                "logs:DescribeLogStreams",
              ],
              resources: [
                `arn:aws:logs:${this.region}:${this.account}:log-group:${logGroup.logGroupName}:log-stream:*`
              ]
            }),
            new PolicyStatement({
              effect: Effect.ALLOW,
              actions: [
                "logs:PutLogEvents"
              ],
              resources: [
                `arn:aws:logs:${this.region}:${this.account}:log-group:${logGroup.logGroupName}:log-stream:${logStream.logStreamName}`
              ]
            }),
            new PolicyStatement({
              actions: [
                "s3:GetObject*"
              ],
              resources: [
                `${artifactBucketArn}/kda-app.zip`
              ]
            })
          ]
        })
      }
    })
  }

  kdaApp = (role: Role, artifactBucketArn: string, srcStream: Stream, destBucket: Bucket, logGroup: LogGroup, logStream: LogStream, appHash: string) => {
    const app = new analytics.CfnApplicationV2(this, 'TestAnalyticsApplication', {
      applicationName: "test-pyflink-application",
      applicationDescription: "test",
      runtimeEnvironment: "FLINK-1_11",
      serviceExecutionRole: role.roleArn,
      applicationConfiguration: {
        flinkApplicationConfiguration: {
          checkpointConfiguration: {
            configurationType: "CUSTOM",
            checkpointingEnabled: true,
            checkpointInterval: 60000,
            minPauseBetweenCheckpoints: 5000,
          },
          monitoringConfiguration: {
            configurationType: "CUSTOM",
            logLevel: "INFO",
            metricsLevel: "APPLICATION",
          },
          parallelismConfiguration: {
            autoScalingEnabled: false,
            configurationType: "CUSTOM",
            parallelism: 1,
            parallelismPerKpu: 1,
          }
        },
        applicationCodeConfiguration: {
          codeContent: {
            s3ContentLocation: {
              bucketArn: artifactBucketArn,
              fileKey: "kda-app.zip",
            },
          },
          codeContentType: "ZIPFILE"
        },
        environmentProperties: {
          propertyGroups: [{
            propertyGroupId: "kinesis.analytics.flink.run.options",
            propertyMap: {
              "python": "kda-app.py",
              "jarfile": "amazon-kinesis-sql-connector-flink-2.0.3.jar",
            }
          }, {
            propertyGroupId: "consumer.config.0",
            propertyMap: {
              "input.stream.name": srcStream.streamName,
              "aws.region": this.region,
              "flink.stream.initpos": "LATEST",
            }
          }, {
            propertyGroupId: "sink.config.0",
            propertyMap: {
              "output.bucket.name": destBucket.bucketName,
            }
          }, {
            propertyGroupId: "meta",
            propertyMap: {
              // force to update the resoruce when code is modified
              "hash": appHash
            }
          }]
        }
      }
    })
    const logOption = new analytics.CfnApplicationCloudWatchLoggingOptionV2(this, 'ApplicationCloudWatchLogggingOption', {
      applicationName: app.applicationName!,
      cloudWatchLoggingOption: {
        logStreamArn: `arn:aws:logs:${this.region}:${this.account}:log-group:${logGroup.logGroupName}:log-stream:${logStream.logStreamName}`
      }
    })
    logOption.addDependsOn(app)
    return app
  }
}
