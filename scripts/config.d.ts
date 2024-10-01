// your-module-name.d.ts

declare const monorepoRoot: string;

declare function getDeploymentEnvBasedOnGitBranch(): string;

export interface DeploymentConfig {
  appName: string;
  account: string;
  etlConfigBase64: string;
  deploymentEnv: string;
  profile?: string;
  regions: string[];
  requireApproval: "never" | "anyChange" | "broadening";
  glueJobTimeoutMinutes: number;
}

declare const appName: string;
declare const account: string;
declare const deploymentEnv: string;
declare const etlConfigBase64: string;
declare const GLUE_WORKFLOW_NAME: string;
declare const profile: string;
declare const regions: string[];
declare const requireApproval: "never" | "anyChange" | "broadening";
declare const glueJobTimeoutMinutes: number;
declare const stackName: string;

export {
  appName,
  account,
  deploymentEnv,
  etlConfigBase64,
  GLUE_WORKFLOW_NAME,
  profile,
  regions,
  requireApproval,
  glueJobTimeoutMinutes,
  monorepoRoot,
  getDeploymentEnvBasedOnGitBranch,
  stackName,
};
