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
  glueJobTimeoutMinutes,
} from "../scripts/config";

regions.forEach((region: string) => {
  const app = new cdk.App();
  new NbiOpenDataAnalyticsStack(
    app,
    `${appName}PinpointStack${deploymentEnv}`,
    {
      appName,
      deploymentEnv,
      glueJobTimeoutMinutes,
      env: {
        account,
        region,
      },
      description: `Pinpoint stack for ${appName}, for ${deploymentEnv} environment created using CDK`,
    }
  );
});
