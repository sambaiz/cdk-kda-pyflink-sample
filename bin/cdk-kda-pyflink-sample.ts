#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from '@aws-cdk/core';
import { CdkKdaPyFlinkSampleStack } from '../lib/cdk-kda-pyflink-sample-stack';

const app = new cdk.App();
new CdkKdaPyFlinkSampleStack(app, 'CdkKdaFlinkSampleStack');
