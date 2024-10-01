// Define the return type for buildETLPackage function
interface ETLPackage {
  etlDistName: string;
  etlDependencies: string;
}

/**
 * Parses pyproject.toml to extract the ETL package name and version.
 *
 * @returns {string} - The name of the ETL package in wheel file format.
 */
export function getETLPackageName(): string;

/**
 * Runs `poetry export` to extract the ETL dependencies as a comma-separated string.
 *
 * @returns {string} - The ETL dependencies in a format suitable for AWS Glue job's `--additional-python-modules` argument.
 */
export function getETLDependencies(): string;

/**
 * Builds the ETL package by running `poetry run build` and returns the package name and dependencies.
 *
 * @returns {ETLPackage} - An object containing the ETL package name and its dependencies.
 */
export function buildETLPackage(): ETLPackage;
