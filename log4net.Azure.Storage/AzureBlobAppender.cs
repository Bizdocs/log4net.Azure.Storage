using log4net.Appender;
using log4net.Azure.Storage.Extensions;
using log4net.Core;
using Microsoft.Extensions.Configuration;
using System;
using System.IO;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using System.Globalization;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;


namespace log4net.Azure.Storage
{
    public class AzureBlobAppender : BufferingAppenderSkeleton
    {
        private BlobContainerClient container;

        public string ConnectionStringName { get; set; }
        public string ConnectionString { get; set; }
        public string ContainerName { get; set; }
        public string DirectoryName { get; set; }
        public string FileName { get; set; }
        public string DatePattern { get; set; }

        public override void ActivateOptions()
        {
            base.ActivateOptions();

            var connectionString = ConnectionString ?? GetConnectionString();

            try
            {
                container = new BlobContainerClient(connectionString, ContainerName.ToLower());
                container.CreateIfNotExists();

            }
            catch (Exception e)
            {
                throw new ArgumentException("Missing or malformed connection string.", nameof(connectionString), e);
            }

        }

        protected override void SendBuffer(LoggingEvent[] events)
        {
            Parallel.ForEach(events, ProcessEvent);
        }

        private void ProcessEvent(LoggingEvent loggingEvent)
        {
            try
            {
                string blobPath = Path.Combine(DirectoryName,
                    $"{FileName}{DateTime.Now.ToString(this.DatePattern, DateTimeFormatInfo.InvariantInfo)}");
             
                try
                {
                    var blobClient = container.GetAppendBlobClient(blobPath);
                    blobClient.CreateIfNotExists();
                    var message = loggingEvent.GetFormattedString(Layout);
                    using MemoryStream memoryStream = new MemoryStream(Encoding.UTF8.GetBytes(message));
                    blobClient.AppendBlock(memoryStream);
                }
                catch (Exception ex)
                { }


            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                throw;
            }
        }

        private string GetConnectionString()
        {
            try
            {
                var basePath = Directory.GetCurrentDirectory();
                var builder = new ConfigurationBuilder()
                    .SetBasePath(basePath)
                    .AddJsonFile("appsettings.json")
                    .AddEnvironmentVariables();

                var config = builder.Build();
                return config.GetConnectionString(ConnectionStringName);
            }
            catch (Exception)
            {
                return "";
            }
        }
    }
}