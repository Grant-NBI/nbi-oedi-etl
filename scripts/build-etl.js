const path = require('path');
const { spawnSync } = require('child_process');
const fs = require('fs');
const smolToml = require('smol-toml'); // Using smol-toml for parsing pyproject.toml

// Define a set of AWS Glue built-in packages to exclude
//https://docs.aws.amazon.com/glue/latest/dg/add-job-python.html
const AWS_BUILT_IN_PACKAGES = new Set([
    'avro', 'awscli', 'awswrangler', 'botocore', 'boto3', 'elasticsearch',
    'numpy', 'pandas', 'psycopg2', 'pyathena', 'PyGreSQL', 'PyMySQL', 'pyodbc',
    'pyorc', 'redshift-connector', 'requests', 'scikit-learn', 'scipy',
    'SQLAlchemy', 's3fs'
]);


const BUILT_IN_PACKAGES_TO_UPDATE = {
    awswrangler: "3.9.1"
}


// Paths
const etlDir = path.join(__dirname, '../etl');
const pyprojectPath = path.join(etlDir, 'pyproject.toml');

// Parse pyproject.toml and extract package information
function getETLPackageName() {
    // Check if pyproject.toml exists
    if (!fs.existsSync(pyprojectPath)) {
        console.error(`pyproject.toml not found at ${pyprojectPath}. Aborting build.`);
        process.exit(1);
    }

    // Parse pyproject.toml to get the package name and version
    const pyprojectContent = fs.readFileSync(pyprojectPath, 'utf-8');
    const pyproject = smolToml.parse(pyprojectContent);

    // Get package name and version from pyproject.toml
    const packageName = pyproject.tool.poetry.name;
    const packageVersion = pyproject.tool.poetry.version;
    return `${packageName}-${packageVersion}-py3-none-any.whl`; // Whl file convention
}

// Grab dependencies from pyproject.toml
function getETLDependencies() {
    console.log('Parsing pyproject.toml to get dependencies...');

    const pyprojectPath = path.join(etlDir, 'pyproject.toml');
    if (!fs.existsSync(pyprojectPath)) {
        console.error(`pyproject.toml not found at ${pyprojectPath}. Aborting.`);
        process.exit(1);
    }

    // Read and parse the pyproject.toml
    const pyprojectContent = fs.readFileSync(pyprojectPath, 'utf-8');
    const pyproject = smolToml.parse(pyprojectContent);

    // Extract production dependencies from [tool.poetry.dependencies]
    const dependencies = pyproject.tool.poetry.dependencies;


    const cleanVersion = (version) => {
        // Strip out the versioning operators (^, ~, >, <, =)
        if (typeof version === 'string') {
            return version.replace(/^[~^><=]+/, '');
        }
        return version;
    };

    // Extract the dependency name and version, exclude built-in packages
    return Object.entries(dependencies)
        .filter(([name]) => name !== 'python' && !AWS_BUILT_IN_PACKAGES.has(name)) // Exclude python and built-ins
        .concat(Object.entries(BUILT_IN_PACKAGES_TO_UPDATE))
        .map(([name, version]) => `${name}==${cleanVersion(version)}`)
        .join(',')

}


// Main build function to generate the wheel and return the package name and dependencies
function buildETLPackage() {
    const etlDistName = getETLPackageName();

    console.log(`ETL Package Name: ${etlDistName}`);

    // Step 1: Run `poetry run build` to generate the wheel
    console.log('Running poetry build to generate the wheel...');
    const buildProcess = spawnSync('poetry', ['run', 'wheel'], {
        env: process.env,
        cwd: etlDir,
        stdio: 'inherit',
        shell: true
    });

    // Check for build errors
    if (buildProcess.error || buildProcess.status !== 0) {
        console.error(`poetry run build failed with exit code ${buildProcess.status}`);
        process.exit(buildProcess.status);
    }

    console.log('Wheel built successfully.');

    // Step 2: Get the dependencies
    const etlDependencies = getETLDependencies();

    console.log(`ETL Dependencies: ${etlDependencies}`);

    // Return the package name and dependencies
    return { etlDistName, etlDependencies };
}

// Export the functions for use in other modules
module.exports = {
    buildETLPackage,
    getETLPackageName,
    getETLDependencies
};

if (require.main === module) {
    const { etlDistName, etlDependencies } = buildETLPackage();
    console.log('Final build information:');
    console.log(`ETL Package Name: ${etlDistName}`);
    console.log(`ETL Dependencies: ${etlDependencies}`);
}
