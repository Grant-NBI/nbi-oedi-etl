const { deploymentEnv, profile, requireApproval } = require('./config')
const join = require('path').join

const cdkCmd = join(__dirname, '../node_modules/.bin/cdk')
const projRoot = join(__dirname, '..')

const { spawn } = require('child_process');

/**
 * Deploys the application using the specified deployment options.
 * @param {Object} options - The deployment options.
 * @param {string} options.cwd - The current working directory.
 * @param {string} options.deploymentEnv - The deployment environment. This can be tied to your branch name.
 * @param {string} [options.profile] - The AWS CLI profile to use.
 * @param {string} [options.requireApproval] - Require approval before deployment (e.g., 'never', 'any-change', 'broadening').
 */
async function deploy(options) {
  const args = ['deploy'];

  if (options.profile && !(process.env.CI === '1' || (process.env.CI && process.env.CI.toLowerCase() === 'true'))) {
    args.push('--profile', options.profile);
  }

  if (options.requireApproval) {
    args.push(`--require-approval`, options.requireApproval);
  }

  try {
    await new Promise((resolve, reject) => {
      console.log(
        `Deploying to environment: ${options.deploymentEnv} with command ${cdkCmd} ${args.join(' ')}`
      );
      const deployProcess = spawn(cdkCmd, args, {
        cwd: options.cwd,
        stdio: 'inherit',
        shell: true,
        env: { ...process.env, FORCE_COLOR: '1' },
      });

      deployProcess.on('error', (err) => {
        console.error(`Failed to start deployment process: ${err}`);
        reject(err);
      });

      deployProcess.on('close', (code) => {
        if (code === 0) {
          console.log(`Deployment successful to environment: ${options.deploymentEnv}`);
          resolve();
        } else {
          console.error(`Deployment failed with exit code ${code}`);
          reject(new Error(`Deployment failed with exit code ${code}`));
        }
      });
    });
  } catch (err) {
    console.error('Deployment process encountered an error:', err);
  }
}

module.exports = { deploy };

deploy({
  cwd: projRoot,
  deploymentEnv,
  profile,
  requireApproval
})