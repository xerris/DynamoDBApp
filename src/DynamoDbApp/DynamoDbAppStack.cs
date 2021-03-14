using Amazon.CDK;
using Amazon.CDK.AWS.APIGateway;
using Amazon.CDK.AWS.DynamoDB;
using Amazon.CDK.AWS.Lambda;
using System.Collections.Generic;

namespace DynamoDbApp
{
    public class DynamoDbAppStack : Stack
    {
        internal DynamoDbAppStack(Construct scope, string id, IStackProps props = null) : base(scope, id, props)
        {
            //Stage setting for Deployment (Need to have Deploy = false in RestApiProps to configure the Stage
            string environment = "PRD";

            //STORAGE INFRASTRUCTURE
            //Create a User DynamoDB Table with a GSI (Primary Key Table)
            var userDynamoDbTable = new Table(this, "User", new TableProps
            {
                TableName = environment + "-" + "User",
                PartitionKey = new Amazon.CDK.AWS.DynamoDB.Attribute
                {
                    Name = "username",
                    Type = AttributeType.STRING
                },
                ReadCapacity = 1,
                WriteCapacity = 1,
                RemovalPolicy = RemovalPolicy.DESTROY
            });

            //Adding the Global Secondary Index (GSI) 
            userDynamoDbTable.AddGlobalSecondaryIndex(new GlobalSecondaryIndexProps
            {
                IndexName = "UserGSIEmailIndex",
                PartitionKey = new Amazon.CDK.AWS.DynamoDB.Attribute
                {
                    Name = "email",
                    Type = AttributeType.STRING
                },
                ReadCapacity = 1,
                WriteCapacity = 1
            });

            //Create a City DynamoDB Table with an LSI (Composite Key Table)
            var cityDynamoDbTable = new Table(this, "City", new TableProps
            {
                TableName = environment + "-" + "City",
                PartitionKey = new Amazon.CDK.AWS.DynamoDB.Attribute
                {
                    Name = "state",
                    Type = AttributeType.STRING
                },
                SortKey = new Amazon.CDK.AWS.DynamoDB.Attribute
                {
                    Name = "city",
                    Type = AttributeType.STRING
                },
                ReadCapacity = 1,
                WriteCapacity = 1,
                RemovalPolicy = RemovalPolicy.DESTROY
            });

            //Adding the Local Secondary Index (LSI)
            cityDynamoDbTable.AddLocalSecondaryIndex(new LocalSecondaryIndexProps
            {
                IndexName = "CityLSIPopulationIndex",
                SortKey = new Amazon.CDK.AWS.DynamoDB.Attribute
                {
                    Name = "population",
                    Type = AttributeType.NUMBER
                }
            });



            //COMPUTE INFRASTRUCTURE
            //User Write DynamoDb Lambda
            var initializeTablesHandler = new Function(this, "initializeTablesHandler", new FunctionProps
            {
                Runtime = Runtime.DOTNET_CORE_3_1,
                FunctionName = "initializeTablesHandler",
                Timeout = Duration.Seconds(20),
                //Where to get the code
                Code = Code.FromAsset("Lambdas\\src\\Lambdas\\bin\\Debug\\netcoreapp3.1"),
                Handler = "Lambdas::Lambdas.Function::InitializeTablesLambdaHandler",
                Environment = new Dictionary<string, string>
                {
                    ["ENVIRONMENT"] = environment,
                    ["USERTABLE"] = userDynamoDbTable.TableName,
                    ["CITYTABLE"] = cityDynamoDbTable.TableName
                }
            });
            userDynamoDbTable.GrantFullAccess(initializeTablesHandler);
            cityDynamoDbTable.GrantFullAccess(initializeTablesHandler);


            //TestPrimaryKeyTable Lambda
            var testPrimaryKeyTableLambdaHandler = new Function(this, "testPrimaryKeyTableLambdaHandler", new FunctionProps
            {
                Runtime = Runtime.DOTNET_CORE_3_1,
                FunctionName = "testPrimaryKeyDynamoDB",
                Timeout = Duration.Seconds(20),
                //Where to get the code
                Code = Code.FromAsset("Lambdas\\src\\Lambdas\\bin\\Debug\\netcoreapp3.1"),
                Handler = "Lambdas::Lambdas.Function::TestPrimaryKeyTableLambdaHandler",
                Environment = new Dictionary<string, string>
                {
                    ["ENVIRONMENT"] = environment,
                    ["USERTABLE"] = userDynamoDbTable.TableName,
                    ["CITYTABLE"] = cityDynamoDbTable.TableName

                }
            });
            userDynamoDbTable.GrantFullAccess(testPrimaryKeyTableLambdaHandler);
            cityDynamoDbTable.GrantFullAccess(testPrimaryKeyTableLambdaHandler);

            //TestCompositeKeyTable Lambda
            var testCompositeKeyTableLambdaHandler = new Function(this, "testCompositeKeyTableLambdaHandler", new FunctionProps
            {
                Runtime = Runtime.DOTNET_CORE_3_1,
                FunctionName = "testCompositeKeyDynamoDBLambda",
                Timeout = Duration.Seconds(20),
                //Where to get the code
                Code = Code.FromAsset("Lambdas\\src\\Lambdas\\bin\\Debug\\netcoreapp3.1"),
                Handler = "Lambdas::Lambdas.Function::TestCompositeKeyTableLambdaHandler",
                Environment = new Dictionary<string, string>
                {
                    ["ENVIRONMENT"] = environment,
                    ["USERTABLE"] = userDynamoDbTable.TableName,
                    ["CITYTABLE"] = cityDynamoDbTable.TableName
                }
            });
            userDynamoDbTable.GrantFullAccess(testCompositeKeyTableLambdaHandler);
            cityDynamoDbTable.GrantFullAccess(testCompositeKeyTableLambdaHandler);


            //This is the name of the API in the APIGateway
            var api = new RestApi(this, "DYNAMODBAPI", new RestApiProps
            {
                RestApiName = "dynamoDBAPI",
                Description = "This our DYNAMODBAPI",
                Deploy = false
            });

            var deployment = new Deployment(this, "My Deployment", new DeploymentProps { Api = api });
            var stage = new Amazon.CDK.AWS.APIGateway.Stage(this, "stage name", new Amazon.CDK.AWS.APIGateway.StageProps
            {
                Deployment = deployment,
                StageName = environment
            });
            api.DeploymentStage = stage;

            //Lambda integrations
            var initializeTablesLambdaIntegration = new LambdaIntegration(initializeTablesHandler, new LambdaIntegrationOptions
            {
                RequestTemplates = new Dictionary<string, string>
                {
                    ["application/json"] = "{ \"statusCode\": \"200\" }"
                }
            });

            var testPrimaryKeyTableLambdaIntegration = new LambdaIntegration(testPrimaryKeyTableLambdaHandler, new LambdaIntegrationOptions
            {
                RequestTemplates = new Dictionary<string, string>
                {
                    ["application/json"] = "{ \"statusCode\": \"200\" }"
                }
            });

            var testCompositeKeyTableLambdaIntegration = new LambdaIntegration(testCompositeKeyTableLambdaHandler, new LambdaIntegrationOptions
            {
                RequestTemplates = new Dictionary<string, string>
                {
                    ["application/json"] = "{ \"statusCode\": \"200\" }"
                }
            });


            //It is up to you if you want to structure your lambdas in separate APIGateway APIs (RestApi)

            //Option 1: Adding at the top level of the APIGateway API
            // api.Root.AddMethod("POST", simpleLambdaIntegration);

            //Option 2: Or break out resources under one APIGateway API as follows
            var initializeTablesResource = api.Root.AddResource("initializetables");
            var initializeTablesMethod = initializeTablesResource.AddMethod("POST", initializeTablesLambdaIntegration);
            var testPrimaryKeyDynamoDBResource = api.Root.AddResource("testprimarykeytable");
            var testPrimaryKeyDynamoDBMethod = testPrimaryKeyDynamoDBResource.AddMethod("POST", testPrimaryKeyTableLambdaIntegration);
            var testCompositeKeyDBResource = api.Root.AddResource("testcompositekeytable");
            var testCompositeKeyDynamoDBMethod = testCompositeKeyDBResource.AddMethod("POST", testCompositeKeyTableLambdaIntegration);

            //Output results of the CDK Deployment
            new CfnOutput(this, "A Region:", new CfnOutputProps() { Value = this.Region });
            new CfnOutput(this, "B UserDynamoDBTable PrimaryKeyTable:", new CfnOutputProps() { Value = userDynamoDbTable.TableName });
            new CfnOutput(this, "C CityDynamoDBTable CompositeKeyTable:", new CfnOutputProps() { Value = cityDynamoDbTable.TableName });

            new CfnOutput(this, "D API Gateway API:", new CfnOutputProps() { Value = api.Url });
            string urlPrefix = api.Url.Remove(api.Url.Length - 1);
            new CfnOutput(this, "E Initialize Tables Lambda:", new CfnOutputProps() { Value = urlPrefix + initializeTablesMethod.Resource.Path });
            new CfnOutput(this, "F Test Primary Key DynamoDB Lambda:", new CfnOutputProps() { Value = urlPrefix + testPrimaryKeyDynamoDBMethod.Resource.Path });
            new CfnOutput(this, "G Test Composite Key DynamoDB Lambda:", new CfnOutputProps() { Value = urlPrefix + testCompositeKeyDynamoDBMethod.Resource.Path });

        }
    }
}