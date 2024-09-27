const path = require('path');
const { spawn } = require('child_process');
const fs = require('fs');


const etlDir = path.join(__dirname, 'etl');
const pyprojectPath = path.join(etlDir, 'pyproject.toml');

if (!fs.existsSync(pyprojectPath)) {
    console.error(`pyproject.toml not found at ${pyprojectPath}. Aborting build.`);
    process.exit(1);
}

// Run `poetry run package_etl` in the scripts/etl directory to build the ETL package
console.log('Running poetry build for ETL package...');

// Spawn the poetry process and inherit stdio to see logs in real-time
const poetryProcess = spawn('poetry', ['run', 'package'], {
    env: process.env,
    cwd: etlDir,
    stdio: 'inherit',
    shell: true
});

// Listen for errors
poetryProcess.on('error', (error) => {
    console.error('Error during packaging:', error);
    process.exit(1);
});

// Listen for process exit and handle non-zero exit codes
poetryProcess.on('close', (code) => {
    if (code !== 0) {
        console.error(`poetry run package_etl failed with exit code ${code}`);
        process.exit(code);
    }
    console.log('Package built successfully.');
});