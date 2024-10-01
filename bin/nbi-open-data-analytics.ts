#!/usr/bin/env node
import "source-map-support/register";
import * as cdk from "aws-cdk-lib";
import { NbiOpenDataAnalyticsStack } from "../lib/nbi-open-data-analytics-stack";
import "source-map-support/register";
import {
  appName,
  account,
  regions,
  deploymentEnv,
  etlConfigBase64,
  GLUE_WORKFLOW_NAME,
  glueJobTimeoutMinutes,
  stackName,
} from "../scripts/config";

regions.forEach((region: string) => {
  const app = new cdk.App();
  new NbiOpenDataAnalyticsStack(app, stackName, {
    appName: appName,
    deploymentEnv,
    etlConfigBase64,
    glueWorkflowName: GLUE_WORKFLOW_NAME,
    glueJobTimeoutMinutes,
    env: {
      account,
      region,
    },
    description: `Oedi Etl stack for ${appName}, for ${deploymentEnv} environment created using CDK`,
  });
});
