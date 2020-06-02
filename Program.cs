using System.Threading.Tasks;

namespace cosmosdb_demo
{
    class Program
    {
        ///use SDKv3
        static async Task Main(string[] args)
        {
            var cosmosDbExample = new CosmosDbExample();
            await cosmosDbExample.GetStartedDemoAsync();
        }
    }
}
