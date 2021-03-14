using System;
using System.Collections;
using System.Collections.Generic;
using System.Net;
using System.Text.Json;
using System.Threading.Tasks;
using Amazon.CDK.AWS.KMS;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using Amazon.Lambda.APIGatewayEvents;
using Amazon.Lambda.Core;
using Newtonsoft.Json.Linq;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace Lambdas
{
    public class Function
    {


        public object InitializeTablesLambdaHandler(object input, ILambdaContext context)
        {
            //basic elements of our response
            bool success = true;
            string message = "";
            string responseText = "";
            string requestBody = "";
            string usersObjectJson = "{}";
            string citiesObjectJson = "{}";

            try
            {
                string environment = Environment.GetEnvironmentVariable("ENVIRONMENT");
                string userTableName = Environment.GetEnvironmentVariable("USERTABLE");
                string cityTableName = Environment.GetEnvironmentVariable("CITYTABLE");
                responseText = "InitializeTablesLambdaHandler CDK Lambda " + DateTime.Now.ToString("dddd, dd MMMM yyyy HH:mm:ss") + " ";
                responseText += "User DynamoDB Table:" + userTableName;
                responseText += "City DynamoDB Table:" + cityTableName;

                var request = JObject.Parse("" + input);
                requestBody = request["body"].ToString();
                var requestBodyJson = JObject.Parse(requestBody);


                //Create the user objects to save
                ArrayList users = new ArrayList();
                User user1 = new User();
                user1.username = "SteveJ";
                user1.firstname = "Steve";
                user1.lastname = "Jamieson";
                user1.email = "steve@gmail.com";
                users.Add(user1);
                User user2 = new User();
                user2.username = "JennyR";
                user2.firstname = "Jenny";
                user2.lastname = "Ross";
                user2.email = "jenny@hotmail.com";
                users.Add(user2);
                User user3 = new User();
                user3.username = "JasonP";
                user3.firstname = "Jason";
                user3.lastname = "Pratt";
                user3.email = "jasonp@outlook.com";
                users.Add(user3);
                User user4 = new User();
                user4.username = "ThomasR";
                user4.firstname = "Thomas";
                user4.lastname = "Richards";
                user4.email = "tomr@abc.com";
                users.Add(user4);


                responseText += " *(1) Populating the User Table(Primary Key)";
                //Inserting a set of users
                DynamoDbUserService dbUserService = new DynamoDbUserService(environment);
                dbUserService.InsertUsers(users);


                responseText += " *(2) Retrieving the User Table(Primary Key) contents";
                List<ScanCondition> conditions = new List<ScanCondition>();
                dbUserService.GetUsersByScan(environment, conditions).Wait();
                usersObjectJson = JsonSerializer.Serialize(dbUserService.users);
                responseText += " # Users=" + dbUserService.users.Count;

                //Create the city objects to save
                ArrayList cities = new ArrayList();
                City city3 = new City();
                city3.state = "California";
                city3.city = "San Diego";
                city3.iscapital = false;
                city3.population = 1423851;
                cities.Add(city3);
                City city2 = new City();
                city2.state = "California";
                city2.city = "Sacramento";
                city2.iscapital = true;
                city2.population = 513624;
                cities.Add(city2);

                City city1 = new City();
                city1.state = "California";
                city1.city = "Los Angeles";
                city1.iscapital = false;
                city1.population = 3792621;
                cities.Add(city1);
                City city4 = new City();
                city4.state = "New York";
                city4.city = "New York";
                city4.iscapital = false;
                city4.population = 8175133;
                cities.Add(city4);
                City city6 = new City();
                city6.state = "New York";
                city6.city = "Buffalo";
                city6.iscapital = false;
                city6.population = 261310;
                cities.Add(city6);

                City city5 = new City();
                city5.state = "New York";
                city5.city = "Albany";
                city5.iscapital = true;
                city5.population = 97478;
                cities.Add(city5);

                responseText += " *(3) Populating the City Table(Composite Key)";
                //Inserting all Cities
                DynamoDbCityService dbCityService = new DynamoDbCityService(environment);
                dbCityService.InsertCities(cities);

                //Let's populate the cities attribute in the response with what is in the table
                responseText += " *(4) Retrieving the City Table(Composite Key) contents";
                conditions = new List<ScanCondition>();
                dbCityService.GetCities(conditions).Wait();
                citiesObjectJson = JsonSerializer.Serialize(dbCityService.cities);
                responseText += " # Cities=" + dbCityService.cities.Count;

            }
            catch (Exception exc)
            {
                success = false;
                message += "InitializeTablesLambdaHandler Exception:" + exc.Message + "," + exc.StackTrace;
            }


            //create the responseBody for the response
            string responseBody = "{";
            responseBody += " \"request\":" + requestBody + ",\n";
            responseBody += " \"response\":\"" + responseText + "\",\n";
            responseBody += " \"users\":" + usersObjectJson + ",\n";
            responseBody += " \"cities\":" + citiesObjectJson + ",\n";
            responseBody += " \"success\":\"" + success + "\",\n";
            responseBody += " \"message\":\"" + message + "\"\n";
            responseBody += "}";

            var response = new APIGatewayProxyResponse
            {
                StatusCode = (int)HttpStatusCode.OK,
                Body = responseBody,
                Headers = new Dictionary<string, string> { { "Content-Type", "text/plain" } }
            };
            return response;
        }



        /** Sample request json
         * {
            }
         */
        public object TestPrimaryKeyTableLambdaHandler(object input, ILambdaContext context)
        {
            //basic elements of our response
            bool success = true;
            string message = "";
            string responseText = "";
            string requestBody = "";
            string usersObjectJson = "{}";
            string citiesObjectJson = "{}";

            try
            {
                string environment = Environment.GetEnvironmentVariable("ENVIRONMENT");
                string userTableName = Environment.GetEnvironmentVariable("USERTABLE");
                string cityTableName = Environment.GetEnvironmentVariable("CITYTABLE");
                responseText = "TestPrimaryKeyTableLambdaHandler CDK Lambda " + DateTime.Now.ToString("dddd, dd MMMM yyyy HH:mm:ss") + " ";
                responseText += "User DynamoDB Table:" + userTableName;
                responseText += "City DynamoDB Table:" + cityTableName;

                var request = JObject.Parse("" + input);
                requestBody = request["body"].ToString();
                var requestBodyJson = JObject.Parse(requestBody);
                DynamoDbUserService dbUserService = new DynamoDbUserService(environment);
                List<ScanCondition> conditions = new List<ScanCondition>();

                dbUserService.GetUsersByScan(environment, conditions).Wait();
                responseText += " *(1) GetUsersByScan(All) collection size =" + dbUserService.users.Count;
                int index = 0;
                foreach (User user in dbUserService.users)
                {
                    responseText += "  User(" + index + "):" + user + "  ";
                    index++;
                }

                //Query Users(with ScanCondition) which username starts with J
                conditions = new List<ScanCondition>();
                conditions.Add(new ScanCondition("username", ScanOperator.BeginsWith, "J"));
                dbUserService.GetUsersByScan(environment, conditions).Wait();
                responseText += " *(2)  GetUsersByScan(UserNames that startWith J) collection size =" + dbUserService.users.Count;
                index = 0;
                foreach (User user in dbUserService.users)
                {
                    responseText += "  User(" + index + "):" + user + "  ";
                    index++;
                }

                //Deleting a User
                string usernameToDelete = "JasonP";
                responseText += " *(3) Delete username=" + usernameToDelete;
                dbUserService.DeleteUserByUserName(usernameToDelete).Wait();
                //responseText += "Delete Log="+dbUserService.log;
                responseText += "Done delete operation";

                //Verify the deletion
                dbUserService.GetUsersByScan(environment, conditions).Wait();
                responseText += " *(4)After Delete collection size =" + dbUserService.users.Count;
                index = 0;
                foreach (User user in dbUserService.users)
                {
                    responseText += "  User(" + index + "):" + user + "  ";
                    index++;
                }

                string searchEmail = "tomr@abc.com";
                //Search via the Partition Key (username)
                responseText += " *(5)Querying user database With Partition Key :" + userTableName;
                dbUserService.GetUser(searchEmail).Wait();
                responseText += "***  GetUser(By Partition Key)=" + searchEmail + " :" + dbUserService.users.Count;
                index = 0;
                foreach (User user in dbUserService.users)
                {
                    responseText += "  User(" + index + "):" + user + "  ";
                    index++;
                }

                //Search via the Global Secondary Index(email)
                responseText += " *(6)Querying user database With GSI :" + userTableName;
                dbUserService.GetUserByEmailGSI(searchEmail).Wait();
                responseText += "GetUserByEmailGSI(By GSI) # Users in database with email=" + searchEmail + " :" + dbUserService.users.Count;
                index = 0;
                foreach (User user in dbUserService.users)
                {
                    responseText += "  User(" + index + "):" + user + "  ";
                    index++;
                }


                //Let's just display the current state of the users table in the json attribute of the users attribute
                responseText += " *(7) Retrieving the current User Table(Primary Key) contents";
                conditions = new List<ScanCondition>();
                dbUserService.GetUsersByScan(environment, conditions).Wait();
                usersObjectJson = JsonSerializer.Serialize(dbUserService.users);
                responseText += " # Users=" + dbUserService.users.Count;
            }
            catch (Exception exc)
            {
                success = false;
                message += "TestPrimaryKeyTableLambdaHandler Exception:" + exc.Message + "," + exc.StackTrace;
            }


            //create the responseBody for the response
            string responseBody = "{";
            responseBody += " \"request\":" + requestBody + ",\n";
            responseBody += " \"response\":\"" + responseText + "\",\n";
            responseBody += " \"users\":" + usersObjectJson + ",\n";
            responseBody += " \"cities\":" + citiesObjectJson + ",\n";
            responseBody += " \"success\":\"" + success + "\",\n";
            responseBody += " \"message\":\"" + message + "\"\n";
            responseBody += "}";

            var response = new APIGatewayProxyResponse
            {
                StatusCode = (int)HttpStatusCode.OK,
                Body = responseBody,
                Headers = new Dictionary<string, string> { { "Content-Type", "text/plain" } }
            };
            return response;

        }

        /* 
  *  
  *  {
     }
  */
        public object TestCompositeKeyTableLambdaHandler(object input, ILambdaContext context)
        {

            //basic elements of our response
            bool success = true;
            string message = "";
            string responseText = "";
            string requestBody = "";
            string usersObjectJson = "{}";
            string citiesObjectJson = "{}";

            try
            {
                string environment = Environment.GetEnvironmentVariable("ENVIRONMENT");
                string userTableName = Environment.GetEnvironmentVariable("USERTABLE");
                string cityTableName = Environment.GetEnvironmentVariable("CITYTABLE");
                responseText = "TestCompositeKeyTableLambdaHandler CDK Lambda " + DateTime.Now.ToString("dddd, dd MMMM yyyy HH:mm:ss") + " ";
                responseText += "User DynamoDB Table:" + userTableName;
                responseText += "City DynamoDB Table:" + cityTableName;

                var request = JObject.Parse("" + input);
                requestBody = request["body"].ToString();
                var requestBodyJson = JObject.Parse(requestBody);
                DynamoDbCityService dbCityService = new DynamoDbCityService(environment);
                List<ScanCondition> conditions = new List<ScanCondition>();
                //Querying all Cities via Scan

                responseText += "City Table Name :" + cityTableName;
                responseText += " *(1)Retrieving all Cities (by Scan) By Primary Key and default sort key(city)";
                List<ScanCondition> cityConditions = new List<ScanCondition>();
                dbCityService.GetCities(conditions).Wait();
                responseText += "# Cities (by Scan)  By Primary Key and default sort key(city)=" + dbCityService.cities.Count;
                int index = 0;
                foreach (City city in dbCityService.cities)
                {
                    responseText += "  City(" + index + "):" + city + "  ";
                    index++;
                }

                //Querying(Scan) all Cities with a population of at least 1 million
                conditions = new List<ScanCondition>();
                conditions.Add(new ScanCondition("population", ScanOperator.GreaterThanOrEqual, 1000000));
                dbCityService.GetCities(conditions).Wait();
                responseText += " *(2) Cities with population greater than 1 million (by Scan) =" + dbCityService.cities.Count;
                index = 0;
                foreach (City city in dbCityService.cities)
                {
                    responseText += "  City(" + index + "):" + city + "  ";
                    index++;
                }

                //Query all of New York Cities(using Partition Key and default Sort Key)
                string state = "New York";
                responseText += " *(3) Get all of cities in the state of:" + state + " using the default sort key(Ascending)";
                dbCityService.GetCitiesByStateUsingDefaultSortKey(state, true).Wait();
                //responseText += "Log = " + dbCityService.log;
                responseText += "# Cities(By Primary Key) in the state of " + state + "=" + dbCityService.cities.Count + " using the default sort key(Ascending)";
                index = 0;
                foreach (City city in dbCityService.cities)
                {
                    responseText += "City(" + index + "):" + city + "  ";
                    index++;
                }

                responseText += " *(4) Get all of cities in the state of:" + state + " using the default sort key(Descending)";
                dbCityService.GetCitiesByStateUsingDefaultSortKey(state, false).Wait();
                //responseText += "Log = " + dbCityService.log;
                responseText += "# Cities(By Primary Key) in the state of " + state + "=" + dbCityService.cities.Count + " using the default sort key(Descending)";
                index = 0;
                foreach (City city in dbCityService.cities)
                {
                    responseText += "City(" + index + "):" + city + "  ";
                    index++;
                }

                //Query all New York Cities (Using Partition Key but use the Population LSI (Ascending Order)
                responseText += " *(5) Get all of cities in the state of:" + state + " using the Population Local Secondary Index(Ascending)";
                dbCityService.GetCitiesByStateUsingPopulationLSI(state, true).Wait();
                //responseText += "Log = " + dbCityService.log;
                responseText += "# Cities(By Primary Key) in the state of " + state + "=" + dbCityService.cities.Count + " using the Population Local Secondary Index (Ascending)";
                index = 0;
                foreach (City city in dbCityService.cities)
                {
                    responseText += "City(" + index + "):" + city + "  ";
                    index++;
                }

                //Query all New York Cities (Using Partition Key but use the Population LSI (Ascending Order)
                responseText += " *(6) Get all of cities in the state of:" + state + " using the Population Local Secondary Index(Descending)";
                dbCityService.GetCitiesByStateUsingPopulationLSI(state, false).Wait();
                //responseText += "Log = " + dbCityService.log;
                responseText += "# Cities(By Primary Key) in the state of " + state + "=" + dbCityService.cities.Count + " using the Population Local Secondary Index(Descending)";
                index = 0;
                foreach (City city in dbCityService.cities)
                {
                    responseText += "City(" + index + "):" + city + "  ";
                    index++;
                }

                //Now demonstrate a delete
                string stateToDelete = "New York";
                string cityToDelete = "Albany";
                responseText += " *(7) Now trying to delete the city with the composite key of state=" + stateToDelete + " and city=" + cityToDelete;
                dbCityService.DeleteCitybyCompositeKey(stateToDelete, cityToDelete).Wait();

                responseText += " *(8) (Verify the Delete) Get all of cities in the state of:" + state + " using the Population Local Secondary Index(Descending)";
                dbCityService.GetCitiesByStateUsingPopulationLSI(state, false).Wait();
                //responseText += "Log = " + dbCityService.log;
                responseText += "# Cities(By Primary Key) in the state of " + state + "=" + dbCityService.cities.Count + " using the Population Local Secondary Index(Descending)";
                index = 0;
                foreach (City city in dbCityService.cities)
                {
                    responseText += "City(" + index + "):" + city + "  ";
                    index++;
                }


                //Let's just display the current state of the cities table in the json attribute of the users attribute
                responseText += " *(9) Retrieving the City Table(Composite Key) contents";
                conditions = new List<ScanCondition>();
                dbCityService.GetCities(conditions).Wait();
                citiesObjectJson = JsonSerializer.Serialize(dbCityService.cities);
                responseText += " # Cities=" + dbCityService.cities.Count;

            }
            catch (Exception exc)
            {
                success = false;
                message += "TestCompositeKeyTableLambdaHandler Exception:" + exc.Message + "," + exc.StackTrace;
            }


            //create the responseBody for the response
            string responseBody = "{";
            responseBody += " \"request\":" + requestBody + ",\n";
            responseBody += " \"response\":\"" + responseText + "\",\n";
            responseBody += " \"users\":" + usersObjectJson + ",\n";
            responseBody += " \"cities\":" + citiesObjectJson + ",\n";
            responseBody += " \"success\":\"" + success + "\",\n";
            responseBody += " \"message\":\"" + message + "\"\n";
            responseBody += "}";

            var response = new APIGatewayProxyResponse
            {
                StatusCode = (int)HttpStatusCode.OK,
                Body = responseBody,
                Headers = new Dictionary<string, string> { { "Content-Type", "text/plain" } }
            };
            return response;
        }

    }

    public class DynamoDbClientService
    {
        private static AmazonDynamoDBClient dynamoDbClient = null;
        public static AmazonDynamoDBClient getDynamoDBClient(string environment)
        {
            if (dynamoDbClient == null)
            {
                dynamoDbClient = new AmazonDynamoDBClient();
            }
            return dynamoDbClient;
        }

    }

    public class DynamoDbUserService
    {
        private string environment = null;
        private DynamoDBContext dynamoDbContext = null;
        private string tableName = null;
        private string GSIEmailIndex = "UserGSIEmailIndex";

        public ArrayList users { get; set; }

        public string log { get; set; }


        public DynamoDbUserService(string environment)
        {
            this.environment = environment;
            DynamoDBContextConfig config = new DynamoDBContextConfig()
            {
                TableNamePrefix = environment + "-"
            };
            dynamoDbContext = new DynamoDBContext(DynamoDbClientService.getDynamoDBClient(environment), config);
            tableName = environment + "-User";
        }

        public void InsertUsers(ArrayList users)
        {
            for (int i = 0; i < users.Count; i++)
            {
                this.InsertUser((User)users[i]);
            }
        }

        public void InsertUser(User user)
        {
            dynamoDbContext.SaveAsync(user);
        }

        public async Task DeleteUserByUserName(string username)
        {
            var Req = new Amazon.DynamoDBv2.Model.DeleteItemRequest
            {
                TableName = this.tableName,
                Key = new Dictionary<string, Amazon.DynamoDBv2.Model.AttributeValue>() { { "username", new Amazon.DynamoDBv2.Model.AttributeValue { S = username.ToString() } } }
            };
            var response = await DynamoDbClientService.getDynamoDBClient(environment).DeleteItemAsync(Req);
            var attributeList = response.Attributes;
        }

        public async Task GetUserByEmailGSI(string email)
        {
            try
            {
                QueryRequest queryRequest = new QueryRequest
                {
                    TableName = this.tableName,
                    IndexName = this.GSIEmailIndex,
                    KeyConditionExpression = "#email = :email",
                    //placeholder that you use in an Amazon DynamoDB expression as an alternative to an actual attribute name
                    ExpressionAttributeNames = new Dictionary<String, String> {
                        {"#email", "email"}
                    },
                    //substitutes for the actual values that you want to compare
                    ExpressionAttributeValues = new Dictionary<string, AttributeValue> {
                    {":email", new AttributeValue { S =  email }}
                    },
                    ScanIndexForward = true
                };

                var response = await DynamoDbClientService.getDynamoDBClient(environment).QueryAsync(queryRequest);
                var items = response.Items;

                this.users = new ArrayList();
                User currentUser = null;
                foreach (var currentItem in items)
                {
                    currentUser = new User();
                    foreach (string attr in currentItem.Keys)
                    {
                        if (attr == "username") { currentUser.username = currentItem[attr].S; }
                        if (attr == "firstname") { currentUser.firstname = currentItem[attr].S; };
                        if (attr == "lastname") { currentUser.lastname = currentItem[attr].S; };
                        if (attr == "email") { currentUser.email = currentItem[attr].S; };
                    }
                    this.users.Add(currentUser);
                }
                //this.log += "F";

            }
            catch (Exception exc)
            {
                this.log += "EXC: " + exc.Message + ":" + exc.StackTrace;
            }
        }

        public async Task GetUser(string email)
        {
            var user = await this.dynamoDbContext.LoadAsync<User>(email);
            users = new ArrayList();
            if (user != null)
            {
                users.Add(user);
            }
        }


        public async Task GetUsersByScan(string environment, List<ScanCondition> conditions)
        {
            var scanUsers = await this.dynamoDbContext.ScanAsync<User>(conditions).GetRemainingAsync();

            this.users = new ArrayList();
            foreach (User u in scanUsers)
            {
                users.Add(u);
            }
        }

    }

    [DynamoDBTable("User")]
    public class User
    {
        [DynamoDBHashKey]
        public string username { get; set; }
        [DynamoDBProperty("firstname")]
        public string firstname { get; set; }
        [DynamoDBProperty("lastname")]
        public string lastname { get; set; }
        [DynamoDBProperty("email")]
        public string email { get; set; }

        public override string ToString()
        {
            return "User: " + username + "," + firstname + "," + lastname + "," + email;
        }
    }


    public class DynamoDbCityService
    {
        private string environment = null;
        private DynamoDBContext dynamoDbContext = null;
        private string tableName = null;
        private string LSIPopulationIndex = "CityLSIPopulationIndex";

        public ArrayList cities { get; set; }

        public string log { get; set; }

        public DynamoDbCityService(string environment)
        {
            this.environment = environment;
            DynamoDBContextConfig config = new DynamoDBContextConfig()
            {
                TableNamePrefix = environment + "-"
            };
            dynamoDbContext = new DynamoDBContext(DynamoDbClientService.getDynamoDBClient(environment), config);
            tableName = environment + "-City";
        }

        public void InsertCities(ArrayList cities)
        {
            for (int i = 0; i < cities.Count; i++)
            {
                this.InsertCity((City)cities[i]);
            }
        }

        public void InsertCity(City city)
        {
            dynamoDbContext.SaveAsync(city);
        }

        public async Task GetCitiesByStateUsingDefaultSortKey(string state, bool ascendingOrder)
        {
            try
            {
                //Note: For this Query Request
                //Sometimes you might encounter an exception such as:
                //Invalid KeyConditionExpression: Attribute name is a reserved keyword; reserved keyword: state:
                //It is for this reason that you will need to include the ExpressionAttributeNames below 
                //(for the KeyConditionExpression and the ProjectionExpression

                //You can note that the records that are pulled for each state will be sorted by the default sort key(city)
                //ProjectExpression identifies the attributes that you want

                var request = new QueryRequest
                {
                    TableName = this.tableName,
                    KeyConditionExpression = "#state = :state",
                    ExpressionAttributeNames = new Dictionary<string, string>
                    {
                        { "#state", "state" },
                    },
                    ExpressionAttributeValues = new Dictionary<string, AttributeValue> {
                        {":state", new AttributeValue { S =  state }}
                    },
                    ProjectionExpression = "#state, city, iscapital, population",
                    ScanIndexForward = ascendingOrder,
                    ConsistentRead = true
                };

                var response = await DynamoDbClientService.getDynamoDBClient(environment).QueryAsync(request);
                this.cities = new ArrayList();
                var items = response.Items;

                City currentCity = null;
                foreach (var currentItem in items)
                {
                    currentCity = new City();
                    foreach (string attr in currentItem.Keys)
                    {
                        if (attr == "state") { currentCity.state = currentItem[attr].S; }
                        if (attr == "city") { currentCity.city = currentItem[attr].S; };
                        if (attr == "iscapital") { currentCity.iscapital = currentItem[attr].BOOL; };
                        if (attr == "population") { currentCity.population = int.Parse(currentItem[attr].N); };
                    }
                    this.cities.Add(currentCity);

                }

            }
            catch (Exception exc)
            {
                this.log += "Exception: " + exc.Message + ":" + exc.StackTrace;
            }
            this.log += "D";

        }


        public async Task DeleteCitybyCompositeKey(string state, string city)
        {
            var deleteItemRequest = new Amazon.DynamoDBv2.Model.DeleteItemRequest
            {
                TableName = this.tableName,
                Key = new Dictionary<string, Amazon.DynamoDBv2.Model.AttributeValue>()
                {
                    { "state", new Amazon.DynamoDBv2.Model.AttributeValue { S = state.ToString() } },
                    { "city", new Amazon.DynamoDBv2.Model.AttributeValue { S = city.ToString() } }
                }
            };
            var response = await DynamoDbClientService.getDynamoDBClient(environment).DeleteItemAsync(deleteItemRequest);
            var attributeList = response.Attributes;

        }


        public async Task GetCitiesByStateUsingPopulationLSI(string state, bool ascendingOrder)
        {
            try
            {
                //Note: For this Query Request
                //Sometimes you might encounter an exception such as:
                //Invalid KeyConditionExpression: Attribute name is a reserved keyword; reserved keyword: state:
                //It is for this reason that you will need to include the ExpressionAttributeNames below 
                //(for the KeyConditionExpression and the ProjectionExpression

                //You can note that the records that are pulled for each state will be sorted by the default sort key(city)

                //ProjectExpression identifies the attributes that you want

                //ScanIndexForward will determine if you will use Ascending or descending order on the Sort Key(the LSI in this case)
                var request = new QueryRequest
                {
                    TableName = this.tableName,
                    IndexName = this.LSIPopulationIndex,
                    KeyConditionExpression = "#state = :state",
                    ExpressionAttributeNames = new Dictionary<string, string>
                    {
                        { "#state", "state" },
                    },
                    ExpressionAttributeValues = new Dictionary<string, AttributeValue> {
                        {":state", new AttributeValue { S =  state }}
                    },
                    ProjectionExpression = "#state, city, iscapital, population",
                    ScanIndexForward = ascendingOrder,
                    ConsistentRead = true
                };

                var response = await DynamoDbClientService.getDynamoDBClient(environment).QueryAsync(request);
                this.cities = new ArrayList();
                var items = response.Items;

                City currentCity = null;
                foreach (var currentItem in items)
                {
                    currentCity = new City();
                    foreach (string attr in currentItem.Keys)
                    {
                        if (attr == "state") { currentCity.state = currentItem[attr].S; }
                        if (attr == "city") { currentCity.city = currentItem[attr].S; };
                        if (attr == "iscapital") { currentCity.iscapital = currentItem[attr].BOOL; };
                        if (attr == "population") { currentCity.population = int.Parse(currentItem[attr].N); };
                    }
                    this.cities.Add(currentCity);
                }
            }
            catch (Exception exc)
            {
                this.log += "Exception: " + exc.Message + ":" + exc.StackTrace;
            }
        }



        public async Task GetCities(List<ScanCondition> conditions)
        {
            var scanCities = await dynamoDbContext.ScanAsync<City>(conditions).GetRemainingAsync();
            this.cities = new ArrayList();
            foreach (City city in scanCities)
            {
                this.cities.Add(city);
            }
        }
    }



    [DynamoDBTable("City")]
    public class City
    {
        [DynamoDBHashKey]
        public string state { get; set; }

        [DynamoDBRangeKey]
        public string city { get; set; }

        [DynamoDBProperty("iscapital")]
        public bool iscapital { get; set; }

        [DynamoDBProperty("population")]
        public int population { get; set; }

        public override string ToString()
        {
            return "City: " + state + "," + city + ",Capital:" + iscapital + ",Population:" + population;
        }
    }

}