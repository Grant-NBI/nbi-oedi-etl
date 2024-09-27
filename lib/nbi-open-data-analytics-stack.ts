import * as cdk from "aws-cdk-lib";
import * as athena from "aws-cdk-lib/aws-athena";
import * as glue from "aws-cdk-lib/aws-glue";
import * as iam from "aws-cdk-lib/aws-iam";
import * as s3 from "aws-cdk-lib/aws-s3";
import { Construct } from "constructs";
import * as fs from "fs";
import * as path from "path";
import * as s3deploy from "aws-cdk-lib/aws-s3-deployment";

export interface NbiOpenDataAnalyticsStackProps extends cdk.StackProps {
  appName: string;
  deploymentEnv: string;
  glueJobTimeoutMinutes?: number;
}

export class NbiOpenDataAnalyticsStack extends cdk.Stack {
  private appName: string;
  private athenaExecutionRole: iam.Role;
  private athenaWorkgroup: athena.CfnWorkGroup;
  private bucket: s3.Bucket;
  private bucketName: string;
  private deploymentEnv: string;
  private glueDbName: string;
  private glueDatabase: glue.CfnDatabase;
  private glueTableName: string;
  private glueTable: glue.CfnTable;
  private glueJobTimeoutSeconds: string;

  constructor(
    scope: Construct,
    id: string,
    props: NbiOpenDataAnalyticsStackProps
  ) {
    super(scope, id, props);

    const { appName, deploymentEnv } = props;

    this.appName = appName.toLowerCase();
    this.deploymentEnv = deploymentEnv.toLowerCase();
    this.glueJobTimeoutSeconds = (
      props?.glueJobTimeoutMinutes || 240 * 60
    ).toString();

    // S3 (shared by ETL scripts + output, and Athena queries results)
    this.createNbiAnalyticsStore();

    //glue etl job
    this.createGlueJob();

    // Glue DB and Table
    this.createGlueResources();

    // Athena Workgroup and Named Queries
    this.createAthenaResources();

    // Glue Crawler to handle schema discovery
    this.createGlueCrawler();
  }

  private createNbiAnalyticsStore() {
    this.bucketName = `${this.appName}-bucket-${this.account}-${this.region}-${this.deploymentEnv}`;
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
                    transitionAfter: cdk.Duration.days(90),
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
                expiration: cdk.Duration.days(30),
              },
            ],
    });
  }

  private createGlueJob() {
    // s3 location: the bucket name is not static so we have to upload it during deployment.
    const pythonScriptPath = `s3://${this.bucket.bucketName}/scripts/etl/oedi-etl/glue_job.py`;

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
    // Upload etl script/packaging
    new s3deploy.BucketDeployment(this, "DeployETLScript", {
      sources: [
        s3deploy.Source.data(
          "oedi-etl.zip",
          path.join(__dirname, "../scripts/etl/package/oedi-etl.zip")
        ),
        s3deploy.Source.asset(
          path.join(__dirname, "../scripts/etl/glue-entry")
        ),
      ],
      destinationBucket: this.bucket,
      destinationKeyPrefix: "scripts/etl/oedi-etl",
      memoryLimit: 1024,
    });

    // job definition
    new glue.CfnJob(this, "ETLGlueJob", {
      role: glueRole.roleArn,
      command: {
        name: "pythonshell",
        pythonVersion: "3.9",
        scriptLocation: pythonScriptPath,
      },
      defaultArguments: {
        "--OUTPUT_BUCKET_NAME": this.bucketName,
        "--job-timeout": this.glueJobTimeoutSeconds,
        "--extra-py-files": `s3://${this.bucket.bucketName}/scripts/etl/oedi-etl/oedi-etl.zip`,
      },
      name: `${this.appName}_etl_job`,
      maxRetries: 1,
      maxCapacity: 1, // 1DPU = 4 vCPU and 16GB memory:
    });
  }

  private createGlueResources() {
    this.glueDbName = `${this.appName}_${this.deploymentEnv}_glue_db`;

    // Glue Database
    this.glueDatabase = new glue.CfnDatabase(this, "GlueDatabase", {
      catalogId: this.account,
      databaseInput: {
        name: this.glueDbName,
      },
    });

    // Glue Table (placeholder, schema and partition will be overwritten dynamically by Glue Crawler)
    //this allows us to have a provisioned table during stack creation while at the same time be flexible enough to match the schema discovered by the crawler - but note that the table is not useful until the crawler runs
    (this.glueTableName = `${this.appName}_etl_output_table`),
      (this.glueTable = new glue.CfnTable(this, "GlueTable", {
        catalogId: this.account,
        databaseName: this.glueDatabase.ref,
        tableInput: {
          name: this.glueTableName,
          storageDescriptor: {
            columns: [
              //placeholder column
              { name: "timestamp", type: "bigint" },
            ],
            location: `s3://${this.bucketName}/etl-outputs/`,
            inputFormat:
              "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            outputFormat:
              "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            compressed: true,
            serdeInfo: {
              serializationLibrary:
                "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
            },
          },
          tableType: "EXTERNAL_TABLE",
          //partitionKeys discovered during schema discovery
        },
      }));
  }

  private createAthenaResources() {
    // Workgroup
    this.athenaWorkgroup = new athena.CfnWorkGroup(this, "AthenaWorkGroup", {
      name: `${this.appName}_workgroup`,
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
    this.athenaWorkgroup.addDependency(this.glueTable);

    this.athenaExecutionRole = new iam.Role(this, "AthenaExecutionRole", {
      assumedBy: new iam.ServicePrincipal("athena.amazonaws.com"),
    });

    // Adding necessary policies for Athena to access S3 and Glue
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
          `arn:aws:glue:${this.region}:${this.account}:catalog`,
          `arn:aws:glue:${this.region}:${this.account}:database/${this.glueDatabase.ref}`,
          `arn:aws:glue:${this.region}:${this.account}:table/${this.glueDatabase.ref}/${this.glueTable.ref}`,
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
        .replace(/\$\s*\{\s*glue_table\s*\}/g, this.glueTableName);

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

  private createGlueCrawler() {
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
        resources: [`arn:aws:s3:::${this.bucketName}/*`],
      })
    );

    new glue.CfnCrawler(this, "GlueCrawler", {
      role: crawlerRole.roleArn,
      databaseName: this.glueDatabase.ref,
      targets: {
        s3Targets: [
          {
            path: `s3://${this.bucketName}/etl-outputs/`,
          },
        ],
      },
      //no schedule: trigger manually
    });
  }
}
