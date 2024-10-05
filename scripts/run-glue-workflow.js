const { GLUE_WORKFLOW_NAME, profile, regions, etlConfigBase64 } = require('./config');
const { GlueClient, StartWorkflowRunCommand } = require('@aws-sdk/client-glue');

// Create an AWS Glue client
// this system can be deployed in multiple regions, but the run only works on one (running the same job in multiple region doesn't make sense => hence use the the first region in the list - if u need to run in diff region, rearrange the order)
const region = regions[0]
process.env.AWS_PROFILE = profile;
process.env.AWS_REGION = region;
const client = new GlueClient({ region });

async function startGlueWorkflow() {
    try {
        // Pass etl_config to simplify python script parsing.
        // NB: this is passed to the glue job via the glue workflow
        // NB. The CDK deployment also pass the argument. This one overrides it per job so that you don't have to change the CDK deployment when u change the etl_config
        //! the console ONLY shows the config available during CDK deployment
        const input = {
            Name: GLUE_WORKFLOW_NAME,
            RunProperties: {
                //overrides
                '--etl_config_override': etlConfigBase64,
            },

        };
        const command = new StartWorkflowRunCommand(input);
        const response = await client.send(command);
        console.log('Workflow started successfully. Run ID:', response.RunId);
    } catch (err) {
        console.error('Error starting the Glue workflow:', err.message);
    }
}

// Run job
startGlueWorkflow(console.log).catch(console.error)

