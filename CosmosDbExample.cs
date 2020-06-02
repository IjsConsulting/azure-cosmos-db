using System;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;

namespace cosmosdb_demo
{
    //ref: 
    //https://github.com/Azure/azure-cosmos-dotnet-v3/blob/master/Microsoft.Azure.Cosmos.Samples/Usage/DatabaseManagement/Program.cs
    //
    public class CosmosDbExample
    {
        readonly string databaseId = "samples";
        readonly string containerId = "container-samples";
        readonly string partitionKey = "/activityId";

        Database database = null;

        public async Task Run()
        {
            try
            {

                string endpoint = "EndPoint";
                if (string.IsNullOrEmpty(endpoint))
                {
                    throw new ArgumentNullException("Please specify a valid endpoint in the appSettings.json");
                }

                string authKey = "AuthorizationKey";
                if (string.IsNullOrEmpty(authKey) || string.Equals(authKey, "Super secret key"))
                {
                    throw new ArgumentException("Please specify a valid AuthorizationKey in the appSettings.json");
                }

                //Read the Cosmos endpointUrl and authorisationKeys from configuration
                //These values are available from the Azure Management Portal on the Cosmos Account Blade under "Keys"
                //NB > Keep these values in a safe & secure location. Together they provide Administrative access to your Cosmos account
                using (CosmosClient client = new CosmosClient(endpoint, authKey))
                {
                    await CreateDatabase(client);
                    await CreateContainer(client);
                }
            }
            catch (CosmosException cre)
            {
                Console.WriteLine(cre.ToString());
            }
            catch (Exception e)
            {
                Exception baseException = e.GetBaseException();
                Console.WriteLine("Error: {0}, Message: {1}", e.Message, baseException.Message);
            }
            finally
            {
                Console.WriteLine("End of demo, press any key to exit.");
                Console.ReadKey();
            }
        }

        private async Task CreateDatabase(CosmosClient client)
        {
            // An object containing relevant information about the response
            DatabaseResponse databaseResponse = await client.CreateDatabaseIfNotExistsAsync(databaseId, 10000);

            // A client side reference object that allows additional operations like ReadAsync
            database = databaseResponse.Database;

            // The response from Azure Cosmos
            DatabaseProperties properties = databaseResponse;

            Console.WriteLine($"\n1. Create a database resource with id: {properties.Id} and last modified time stamp: {properties.LastModified}");
            Console.WriteLine($"\n2. Create a database resource request charge: {databaseResponse.RequestCharge} and Activity Id: {databaseResponse.ActivityId}");

            // Read the database from Azure Cosmos
            DatabaseResponse readResponse = await database.ReadAsync();
            Console.WriteLine($"\n3. Read a database: {readResponse.Resource.Id}");

            await readResponse.Database.CreateContainerAsync("testContainer", "/pk");

            // Get the current throughput for the database
            int? throughputResponse = await database.ReadThroughputAsync();
            if (throughputResponse.HasValue)
            {
                Console.WriteLine($"\n4. Read a database throughput: {throughputResponse}");

                // Update the current throughput for the database
                await database.ReplaceThroughputAsync(11000);
            }

            Console.WriteLine("\n5. Reading all databases resources for an account");
            FeedIterator<DatabaseProperties> iterator = client.GetDatabaseQueryIterator<DatabaseProperties>();
            do
            {
                foreach (DatabaseProperties db in await iterator.ReadNextAsync())
                {
                    Console.WriteLine(db.Id);
                }
            } while (iterator.HasMoreResults);

            // Delete the database from Azure Cosmos.
            await database.DeleteAsync();
            Console.WriteLine($"\n6. Database {database.Id} deleted.");
        }

        private async Task CreateContainer(CosmosClient client)
        {
            await Setup(client);

            Container simpleContainer = await CreateContainer();

            await CreateContainerWithCustomIndexingPolicy();

            await CreateContainerWithTtlExpiration();

            await GetAndChangeContainerPerformance(simpleContainer);

            await ReadContainerProperties();

            await ListContainersInDatabase();

            await DeleteContainer();
        }

        private async Task Setup(CosmosClient client)
        {
            database = await client.CreateDatabaseIfNotExistsAsync(databaseId);
        }

        private async Task<Container> CreateContainer()
        {
            // Set throughput to the minimum value of 400 RU/s
            ContainerResponse simpleContainer = await database.CreateContainerIfNotExistsAsync(
                id: containerId,
                partitionKeyPath: partitionKey,
                throughput: 400);

            Console.WriteLine($"\n1.1. Created container :{simpleContainer.Container.Id}");
            return simpleContainer;
        }

        private async Task CreateContainerWithCustomIndexingPolicy()
        {
            // Create a container with custom index policy (consistent indexing)
            // We cover index policies in detail in IndexManagement sample project
            ContainerProperties containerProperties = new ContainerProperties(
                id: "SampleContainerWithCustomIndexPolicy",
                partitionKeyPath: partitionKey);
            containerProperties.IndexingPolicy.IndexingMode = IndexingMode.Consistent;

            Container containerWithConsistentIndexing = await database.CreateContainerIfNotExistsAsync(
                containerProperties,
                throughput: 400);

            Console.WriteLine($"1.2. Created Container {containerWithConsistentIndexing.Id}, with custom index policy \n");

            await containerWithConsistentIndexing.DeleteContainerAsync();
        }

        private async Task CreateContainerWithTtlExpiration()
        {
            ContainerProperties properties = new ContainerProperties
                (id: "TtlExpiryContainer",
                partitionKeyPath: partitionKey);
            properties.DefaultTimeToLive = (int)TimeSpan.FromDays(1).TotalSeconds; //expire in 1 day

            ContainerResponse ttlEnabledContainerResponse = await database.CreateContainerIfNotExistsAsync(
                containerProperties: properties);
            ContainerProperties returnedProperties = ttlEnabledContainerResponse;

            Console.WriteLine($"\n1.3. Created Container \n{returnedProperties.Id} with TTL expiration of {returnedProperties.DefaultTimeToLive}");

            await ttlEnabledContainerResponse.Container.DeleteContainerAsync();
        }

        private async Task GetAndChangeContainerPerformance(Container simpleContainer)
        {
            int? throughputResponse = await simpleContainer.ReadThroughputAsync();

            Console.WriteLine($"\n2. Found throughput \n{throughputResponse}\nusing container's id \n{simpleContainer.Id}");

            await simpleContainer.ReplaceThroughputAsync(500);

            Console.WriteLine("\n3. Replaced throughput. Throughput is now 500.\n");

            // Get the offer again after replace
            throughputResponse = await simpleContainer.ReadThroughputAsync();

            Console.WriteLine($"3. Found throughput \n{throughputResponse}\n using container's ResourceId {simpleContainer.Id}.\n");
        }

        private async Task ReadContainerProperties()
        {

            Container container = database.GetContainer(containerId);
            ContainerProperties containerProperties = await container.ReadContainerAsync();

            Console.WriteLine($"\n4. Found Container \n{containerProperties.Id}\n");
        }

        private async Task ListContainersInDatabase()
        {
            Console.WriteLine("\n5. Reading all CosmosContainer resources for a database");

            FeedIterator<ContainerProperties> resultSetIterator = database.GetContainerQueryIterator<ContainerProperties>();
            while (resultSetIterator.HasMoreResults)
            {
                foreach (ContainerProperties container in await resultSetIterator.ReadNextAsync())
                {
                    Console.WriteLine(container.Id);
                }
            }
        }

        private async Task DeleteContainer()
        {
            await database.GetContainer(containerId).DeleteContainerAsync();
            Console.WriteLine("\n6. Deleted Container\n");
        }
    }

    public class ToDoActivity
    {
        public string id = null;
        public string activityId = null;
        public string status = null;
    }
}