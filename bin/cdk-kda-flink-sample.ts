#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from '@aws-cdk/core';
import { CdkKdaFlinkSampleStack } from '../lib/cdk-kda-flink-sample-stack';

const app = new cdk.App();
new CdkKdaFlinkSampleStack(app, 'CdkKdaFlinkSampleStack');
