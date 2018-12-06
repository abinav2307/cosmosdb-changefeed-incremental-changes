using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.CosmosDB.ChangeFeedTest
{
    class Program
    {
        static void Main(string[] args)
        {
            ChangeFeedWithOpLogTester changeFeedWithOpLogTester = new ChangeFeedWithOpLogTester();
            changeFeedWithOpLogTester.RunChangeFeedWithOpLogTest().Wait();

            Console.WriteLine("Completed execution!");
            Console.ReadLine();
        }
    }
}
