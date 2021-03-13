using Amazon.CDK;
using System;
using System.Collections.Generic;
using System.Linq;

namespace DynamoDbApp
{
    sealed class Program
    {
        public static void Main(string[] args)
        {
            var app = new App();
            new DynamoDbAppStack(app, "DynamoDbAppStack");
            app.Synth();
        }
    }
}
