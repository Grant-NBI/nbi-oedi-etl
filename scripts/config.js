const path = require('path')
const fs = require('fs');
const config = require('../config.json')

//you would need a system to determine which environment you want to deploy into. Normally, this is tied to a Git Branch. For prod, if u won't do this from the git branch, you can simply manually set the deploymentEnv to 'prod'
//* using monorepo in case it is added to a monorepo (can make this same as the project root)
const monorepoRoot = path.resolve(path.join(__dirname, '../'), config.monorepoRoot)
const getDeploymentEnvBasedOnGitBranch = () => {
    //you can run a git script in exec sync child process if you prefer that :this is a quick way assuming the .git directory is known and present
    try {
        const gitHeadPath = path.join(monorepoRoot, '.git', 'HEAD');
        const headFileContent = fs.readFileSync(gitHeadPath, 'utf8').trim();

        if (headFileContent.startsWith('ref:')) {
            return headFileContent.split('/').pop();
        }

        return 'dev'; // Fallback
    } catch (error) {
        return 'dev'; // Fallback
    };
}

const {
    appName,
    account,
    deploymentEnv,
    profile,
    regions,
    requireApproval,
    glueJobTimeoutMinutes
} = config.deploymentConfig.find(config => config.deploymentEnv === getDeploymentEnvBasedOnGitBranch());

// quick validations
if (!appName || typeof appName !== "string") {
    throw new Error("Invalid appName: must be a non-empty string.");
}

if (!account || typeof account !== "string") {
    throw new Error("Invalid account: must be a non-empty string.");
}

if (!deploymentEnv || typeof deploymentEnv !== "string") {
    throw new Error("Invalid deploymentEnv: must be a non-empty string.");
}



//u won't use ur profile on the CI/CD pipeline if u set up one
if ((!profile || typeof profile !== "string") && !(process.env.CI === "1" || (process.env.CI && process.env.CI.toLowerCase() === "true"))) {
    throw new Error("Invalid profile: must be a non-empty string when not running in a CI environment.");
}


if (!Array.isArray(regions) || regions.length === 0 || !regions.every(region => typeof region === "string")) {
    throw new Error("Invalid regions: must be a non-empty array of strings.");
}

const validApprovalOptions = ["never", "anyChange", "broadening"];
if (!validApprovalOptions.includes(requireApproval)) {
    throw new Error(`Invalid requireApproval: must be one of ${validApprovalOptions.join(", ")}.`);
}

module.exports = {
    appName,
    account,
    deploymentEnv,
    profile,
    regions,
    requireApproval,
    glueJobTimeoutMinutes // TODO! add  validation
};

console.debug(module.exports);
