import * as cdk from "aws-cdk-lib";
import * as athena from "aws-cdk-lib/aws-athena";
import * as glue from "aws-cdk-lib/aws-glue";
import * as iam from "aws-cdk-lib/aws-iam";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as s3deploy from "aws-cdk-lib/aws-s3-deployment";
import { Case } from "change-case-all";
import { Construct } from "constructs";
import * as fs from "fs";
import * as path from "path";
import { buildETLPackage } from "../scripts/build-etl";

export interface NbiOpenDataAnalyticsStackProps extends cdk.StackProps {
  appName: string;
  deploymentEnv: string;
  etlConfigBase64: string;
  glueWorkflowName: string;
  glueJobTimeoutMinutes?: number;
}

export class NbiOpenDataAnalyticsStack extends cdk.Stack {
  private appNameSnake: string;
  private appNameKebab: string;
  private athenaExecutionRole: iam.Role;
  private athenaWorkgroup: athena.CfnWorkGroup;
  private bucket: s3.Bucket;
  private bucketName: string;
  private dataCrawlerName: string;
  private deploymentEnv: string;
  etlConfigBase64: string;
  private glueWorkflowName: string;
  private glueDbName: string;
  private glueDatabase: glue.CfnDatabase;
  private glueJob: glue.CfnJob;
  private glueJobName: string;
  private glueJobTimeoutSeconds: number;
  private metadataCrawlerName: string;
  private oediEtlS3Prefix: string;
  private prefixForDataTable: string;
  private prefixForMetadataTable: string;

  constructor(
    scope: Construct,
    id: string,
    props: NbiOpenDataAnalyticsStackProps
  ) {
    super(scope, id, props);

    const { appName, deploymentEnv, etlConfigBase64, glueWorkflowName } = props;
    const appNameSnakeCase = Case.snake(appName);
    const appNameKebabCase = Case.kebab(appName);
    const deploymentEnvLowerCase = deploymentEnv.toLowerCase();
    this.appNameSnake = appNameSnakeCase;
    this.appNameKebab = appNameKebabCase;
    this.etlConfigBase64 = etlConfigBase64;
    this.glueWorkflowName = glueWorkflowName;
    this.deploymentEnv = deploymentEnv.toLowerCase();
    this.glueJobTimeoutSeconds = props?.glueJobTimeoutMinutes || 240 * 60;

    //*define names. values that need to exist at deployment time and shared around are defined in config.js
    //!Define name that are required to be available during deployment => change will trigger unnecessary CDK updates

    //values that are used internally are defined here
    this.bucketName = `${appNameKebabCase}-${this.account}-${deploymentEnvLowerCase}-oedi-etl-bucket`; //!no underscores and there are other rules
    this.dataCrawlerName = `${appNameSnakeCase}_${deploymentEnvLowerCase}_oedi_glue_data_crawler`;
    this.metadataCrawlerName = `${appNameSnakeCase}_${deploymentEnvLowerCase}_oedi_glue_metadata_crawler`;
    this.glueDbName = `${this.appNameSnake}_${this.deploymentEnv}_oedi_glue_db`;
    this.prefixForDataTable = `${this.appNameSnake}_${this.deploymentEnv}_oedi_data_table_`;
    this.prefixForMetadataTable = `${this.appNameSnake}_${this.deploymentEnv}_oedi_metadata_table_`;

    //!TODO the use of tables is rendered useless ->generated by crawlers during ETL as the system is rearranged to only filter state data and the state value is only known during the job and it doesn't make sense to create tables for all here.  Remove tables at some point (or they can simply be place holders)
    this.oediEtlS3Prefix = `scripts/etl/${deploymentEnvLowerCase}/oedi`; //! do not use oedi_etl in path
    this.glueJobName = `${appNameSnakeCase}_${deploymentEnvLowerCase}_oedi_glue_job`;

    // S3 (shared by ETL scripts + output, and Athena queries results)
    this.createNbiAnalyticsStore();

    //glue etl job
    this.createGlueJob();

    // Glue Crawler to handle schema discovery
    this.createGlueCrawlers();

    //workflow to orchestrate etl
    this.createGlueWorkflow();

    // Glue DB and Table
    this.createGlueResources();

    // Athena Workgroup and Named Queries
    this.createAthenaResources();

    //export
    new cdk.CfnOutput(this, "AnalyticStoreCfnOutput", {
      value: this.bucket.bucketName,
      exportName: `${this.appNameKebab}-oedi-stack-${deploymentEnv}-bucket`,
    });
  }

  private createNbiAnalyticsStore() {
    this.bucket = new s3.Bucket(this, "OutputBucket", {
      bucketName: this.bucketName,
      removalPolicy:
        this.deploymentEnv === "prod"
          ? cdk.RemovalPolicy.RETAIN
          : cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: this.deploymentEnv !== "prod",
      lifecycleRules:
        this.deploymentEnv === "prod"
          ? [
              {
                id: "ProdTransitionToIA",
                enabled: true,
                transitions: [
                  {
                    storageClass: s3.StorageClass.INFREQUENT_ACCESS,
                    transitionAfter: cdk.Duration.days(30),
                  },
                  {
                    storageClass: s3.StorageClass.GLACIER,
                    transitionAfter: cdk.Duration.days(180),
                  },
                ],
              },
              {
                id: "ProdExpireObjects",
                enabled: true,
                expiration: cdk.Duration.days(365), //! delete after 1 year
              },
            ]
          : [
              {
                id: "NonProdExpireObjects",
                enabled: true,
                expiration: cdk.Duration.days(180),
              },
            ],
    });
  }

  private createGlueWorkflow() {
    const glueWorkflowRole = new iam.Role(this, "GlueWorkflowRole", {
      assumedBy: new iam.ServicePrincipal("glue.amazonaws.com"),
    });

    // Attach necessary policies to the role
    glueWorkflowRole.addToPolicy(
      new iam.PolicyStatement({
        actions: [
          // "glue:BatchGetJobs",
          // "glue:BatchGetCrawlers",
          // "glue:BatchGetTriggers",
          // "glue:BatchGetWorkflows",
          // "glue:GetCrawler",
          // "glue:GetCrawlerMetrics",
          // "glue:GetJobRun",
          // "glue:GetJobRuns",
          // "glue:GetTrigger",
          // "glue:GetTriggers",
          // "glue:GetWorkflowRun",
          // "glue:GetWorkflowRuns",
          // "glue:StartCrawler",
          // "glue:StartJobRun",
          // "glue:StartTrigger",
          // "glue:StartWorkflowRun",
          // "glue:UpdateTrigger",
          "glue:*",
        ],
        resources: [
          //TODO: tighten up permissions
          // `arn:aws:glue:${this.region}:${this.account}:workflow/*`,
          // `arn:aws:glue:${this.region}:${this.account}:trigger/*`,
          // `arn:aws:glue:${this.region}:${this.account}:crawler/*`,
          // `arn:aws:glue:${this.region}:${this.account}:job/*`,
          "*",
        ],
      })
    );

    // Glue Workflow
    const workflow = new glue.CfnWorkflow(this, "GlueWorkflow", {
      name: this.glueWorkflowName,
    });

    // Glue Job Trigger (Trigger the job first, and it will proxy the workflow argument by default)
    new glue.CfnTrigger(this, "JobTrigger", {
      type: "ON_DEMAND",
      workflowName: workflow.ref,
      actions: [
        {
          jobName: this.glueJobName,
        },
      ],
    });

    //Data and Metadata Crawlers Trigger
    new glue.CfnTrigger(this, "CrawlersTrigger", {
      type: "CONDITIONAL",
      workflowName: workflow.ref,
      actions: [
        {
          crawlerName: this.dataCrawlerName,
        },
        {
          crawlerName: this.metadataCrawlerName,
        },
      ],
      predicate: {
        conditions: [
          {
            logicalOperator: "EQUALS",
            jobName: this.glueJobName,
            state: "SUCCEEDED",
          },
        ],
      },
    });
  }

  private createGlueJob() {
    const pythonScriptPath = `s3://${this.bucket.bucketName}/${this.oediEtlS3Prefix}/glue_job.py`;

    // role
    const glueRole = new iam.Role(this, "GlueJobRole", {
      assumedBy: new iam.ServicePrincipal("glue.amazonaws.com"),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName(
          "service-role/AWSGlueServiceRole"
        ),
      ],
    });

    this.bucket.grantReadWrite(glueRole);

    //glue also needs to read public datasets
    glueRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ["s3:GetObject", "s3:ListBucket"],
        resources: ["arn:aws:s3::*", "arn:aws:s3:::*/*"],
      })
    );

    glueRole.addToPolicy(
      new iam.PolicyStatement({
        actions: [
          // "glue:UpdateCrawler",
          // "glue:StartTrigger",
          "glue:*",
        ],
        resources: [
          // `arn:aws:glue:*:${this.account}:crawler/${this.dataCrawlerName}`,
          // `arn:aws:glue:*:${this.account}:crawler/${this.metadataCrawlerName}`,
          "*",
        ],
      })
    );
    // Upload etl script/packaging
    // Deploy ETL wheel file and Glue job entry script to S3

    //build python etl package
    const { etlDistName, etlDependencies } = buildETLPackage();
    new s3deploy.BucketDeployment(this, "DeployETLScript", {
      sources: [
        // Deploy the wheel file
        s3deploy.Source.asset(path.join(__dirname, `../etl/dist/`)),
        // Deploy the Glue job entry script
        s3deploy.Source.asset(path.join(__dirname, "../etl/glue-entry")),
      ],
      destinationBucket: this.bucket,
      destinationKeyPrefix: this.oediEtlS3Prefix,
      memoryLimit: 256,
    });

    //default values =>etlConfig will be overridden per run
    const defaultArguments: Record<string, string> = {
      "--DATA_CRAWLER_NAME": this.dataCrawlerName,
      "--ETL_EXECUTION_ENV": "AWS_GLUE",
      "--etl_config": this.etlConfigBase64,
      "--extra-py-files": `s3://${this.bucket.bucketName}/${this.oediEtlS3Prefix}/${etlDistName}`,
      "--METADATA_CRAWLER_NAME": this.metadataCrawlerName,
      "--OUTPUT_BUCKET_NAME": this.bucketName,
      "--TempDir": `s3://${this.bucket.bucketName}/${this.oediEtlS3Prefix}/temp/`,
    };
    if (etlDependencies) {
      defaultArguments["--additional-python-modules"] = etlDependencies;
    }

    // job definition
    this.glueJob = new glue.CfnJob(this, "ETLGlueJob", {
      name: this.glueJobName,
      role: glueRole.roleArn,
      command: {
        name: "pythonshell",
        pythonVersion: "3.9",
        scriptLocation: pythonScriptPath,
      },
      defaultArguments,
      // maxRetries: 1,
      maxCapacity: 1, // 1DPU = 4 vCPU and 16GB memory:
      timeout: this.glueJobTimeoutSeconds,
    });
  }

  private createGlueCrawlers() {
    const crawlerRole = new iam.Role(this, "GlueCrawlerRole", {
      assumedBy: new iam.ServicePrincipal("glue.amazonaws.com"),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName(
          "service-role/AWSGlueServiceRole"
        ),
      ],
    });
    crawlerRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
        resources: [
          `arn:aws:s3:::${this.bucketName}`,
          `arn:aws:s3:::${this.bucketName}/*`,
        ],
      })
    );

    crawlerRole.addToPolicy(
      new iam.PolicyStatement({
        actions: [
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:GetDatabase",
          "glue:GetTable",
          "glue:BatchCreatePartition",
          "glue:CreateDatabase",
        ],
        resources: [
          `arn:aws:glue:${this.region}:${this.account}:catalog`,
          `arn:aws:glue:${this.region}:${this.account}:database/${this.glueDbName}`,
          `arn:aws:glue:${this.region}:${this.account}:table/${this.glueDbName}/*`,
        ],
      })
    );

    //data crawler
    new glue.CfnCrawler(this, "DataCrawler", {
      name: this.dataCrawlerName,
      role: crawlerRole.roleArn,
      databaseName: this.glueDbName,
      tablePrefix: `${this.prefixForDataTable}_`,
      targets: {
        s3Targets: [
          {
            //!Placeholders: will be dynamically updated by glue job
            path: `s3://${this.bucketName}/etl-outputs/`,
          },
        ],
      },
      //*no schedule: trigger manually
      //it's simple though to add a schedule if needed: here is an example
      // schedule: {
      //* scheduleExpression: "cron(0 0 1 1,7 ? *)" //runs jan 1 and july 7
      // }
    });
    new glue.CfnCrawler(this, "MetadataCrawler", {
      name: this.metadataCrawlerName,
      role: crawlerRole.roleArn,
      databaseName: this.glueDbName,
      tablePrefix: `${this.prefixForMetadataTable}_`,
      targets: {
        s3Targets: [
          {
            //!Placeholders: will be dynamically updated by glue job
            path: `s3://${this.bucketName}/etl-outputs/`,
          },
        ],
      },
      //*no schedule: trigger manually
      //it's simple though to add a schedule if needed: here is an example
      // schedule: {
      //* scheduleExpression: "cron(0 0 1 1,7 ? *)" //runs jan 1 and july 7
      // }
    });
  }

  private createGlueResources() {
    // Glue Database
    this.glueDatabase = new glue.CfnDatabase(this, "GlueDatabase", {
      catalogId: this.account,
      databaseInput: {
        name: this.glueDbName,
      },
    });

    //! NO tables are created during CDK deployment. Tables are instead created by crawlers during ETL job execution. The reason is for efficiency purpose. The ETL job filters data by state and the state value is only known during the job execution. It doesn't make sense to create tables for all states here.
  }

  private createAthenaResources() {
    // Workgroup
    this.athenaWorkgroup = new athena.CfnWorkGroup(this, "AthenaWorkGroup", {
      name: `${this.appNameSnake}_workgroup`,
      description: "Workgroup for data analytics",
      state: "ENABLED",
      workGroupConfiguration: {
        resultConfiguration: {
          outputLocation: `s3://${this.bucketName}/query-results/`,
        },
      },
    });

    //* adding explicit dependencies
    this.athenaWorkgroup.addDependency(this.glueDatabase);

    this.athenaExecutionRole = new iam.Role(this, "AthenaExecutionRole", {
      assumedBy: new iam.ServicePrincipal("athena.amazonaws.com"),
    });

    this.athenaExecutionRole.addToPolicy(
      new iam.PolicyStatement({
        actions: [
          "athena:StartQueryExecution",
          "athena:GetQueryResults",
          "glue:GetDatabase",
          "glue:GetTable",
          "glue:GetPartition",
          "glue:BatchCreatePartition",
          "s3:GetObject",
          "s3:ListBucket",
        ],
        resources: [
          `arn:aws:s3:::${this.bucketName}`,
          `arn:aws:s3:::${this.bucketName}/*`,
          `arn:aws:glue:${this.region}:${this.account}:catalog`, // Glue catalog
          `arn:aws:glue:${this.region}:${this.account}:database/${this.glueDatabase.ref}`,
          //! All tables in the Glue database are allowed since we don't create tables here
          `arn:aws:glue:${this.region}:${this.account}:table/${this.glueDatabase.ref}/*`,
        ],
      })
    );

    // named queries
    this.addNamedQueries();
  }

  private addNamedQueries() {
    const sqlFilePath = path.join(__dirname, "../sql/saved-queries.sql");
    const sqlFileContent = fs.readFileSync(sqlFilePath, "utf8");

    //  grab labels and sql statements only (clean comments, empty statements)
    const sqlStatements = sqlFileContent
      .replace(/\/\*[\s\S]*?\*\//g, "") // multiline comments
      .split(/(?:^|\n)(--\s*label)/gm) // split by label lines
      .map((section) => section.trim())
      .filter((section) => section !== "")
      .map((section) => {
        const [labelLine, ...sqlParts] = section.split(/\n/);
        const label = labelLine.replace("-- label:", "").trim();
        const sql = sqlParts.join("\n").trim();
        return { label, sql };
      })
      .filter(({ sql }) => sql);

    // console.debug(1427, sqlStatements);

    // Create named queries by iterating over the sql statements
    sqlStatements.forEach((sqlStatement, index) => {
      const label = sqlStatement.label
        .replace(/[^a-zA-Z0-9\s]/g, "")
        .trim()
        .toLowerCase()
        .split(/\s+/)
        .join("_");
      const query = sqlStatement.sql
        .replace(/--.*\n/g, "")
        .trim()
        .replace(/\$\s*\{\s*glue_db\s*\}/g, this.glueDbName)
        .replace(
          /\$\s*\{\s*glue_data_table_prefix\s*\}/g,
          this.prefixForDataTable
        )
        .replace(
          /\$\s*\{\s*glue_metadata_table_prefix\s*\}/g,
          this.prefixForMetadataTable
        )
        .replace(/_ +/g, "_"); // Remove spaces after underscore

      console.log(1428, query);

      new athena.CfnNamedQuery(
        this,
        `${this.athenaWorkgroup}NamedQuery${index + 1}`,
        {
          database: this.glueDatabase.ref,
          name: label,
          queryString: query,
          workGroup: this.athenaWorkgroup.name,
        }
        //for some reason, CDK can't figure out dependency on athenaWorkgroup
      ).addDependency(this.athenaWorkgroup); //which in turn depends on glueTable and glueDatabase
    });
  }
}
