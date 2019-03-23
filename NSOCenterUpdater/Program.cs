using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using MongoDB.Bson;
using MongoDB.Driver;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;

namespace NSOCenterUpdater
{
    class Program
    {
        static bool[] taks;
        static List<Ea> eaDocuments;
        static List<CloudBlockBlob> blobs;
        static IMongoCollection<BsonDocument> collection;
        const string LogFilePath = "Z:/nsoLog.txt";
        const string StorageConnectionString = "AZURE_STORAGE_CONN_STRING";
        const string MongoDBConnectionString = "MONGO_DB_CONN_STRING";
        const int EaDocumentsPerThread = 10000;

        static void Main(string[] args)
        {
            #region Azure storage
            Console.WriteLine("Connecting to Azure storage");
            var storageAccount = CloudStorageAccount.Parse(StorageConnectionString);
            var client = storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference("byea");
            blobs = container.ListBlobs().Where(it => it != null && it is CloudBlockBlob).Select(it => it as CloudBlockBlob).ToList();
            var blobIds = blobs.Select(it => it.Name);
            Console.WriteLine($"=> Found: {blobs.Count} Blobs.");
            #endregion Azure storage

            #region MongoDb
            Console.WriteLine("Connecting to MongoDb");
            var mongo = new MongoClient(MongoDBConnectionString);
            var mongoDb = mongo.GetDatabase("nsoea");
            collection = mongoDb.GetCollection<BsonDocument>("ea_copy3");
            eaDocuments = collection.Find(it => true).ToEnumerable().Select(it => new Ea
            {
                Id = it.Elements.FirstOrDefault(i => i.Name == "_id").Value.ToString(),
                Document = it
            }).ToList();
            Console.WriteLine($"=> Found: {eaDocuments.Count} ea documents.");
            #endregion MongoDb

            #region Logging
            if (!File.Exists(LogFilePath))
            {
                var writer = File.Create(LogFilePath);
                writer.Dispose();
            }
            using (var file = File.AppendText(LogFilePath))
            {
                file.WriteLine($"Begin: {DateTime.Now}");
            }
            #endregion Logging

            #region Split EA documents to threading
            const int OfferRound = 1;
            var runners = (eaDocuments.Count / EaDocumentsPerThread) + OfferRound;
            Console.WriteLine($"Creating {runners} runners.");
            taks = new bool[runners];
            for (int i = 0, skipDocuments = 0; i < runners; i++)
            {
                var t = new Thread(Runner);
                t.Start(new RunnerParam { Skip = skipDocuments, Take = EaDocumentsPerThread, Id = i });
                skipDocuments += EaDocumentsPerThread;
            }
            #endregion Split EA documents to threading

            Console.ReadLine();
        }

        private static void Runner(object param)
        {
            var rp = param as RunnerParam;

            #region Calculate center
            var calculationRound = 0d;
            var updateEADic = new Dictionary<string, dynamic>();
            var eaQry = eaDocuments.Skip(rp.Skip).Take(rp.Take);
            foreach (var item in eaQry)
            {
                var selectedBlob = blobs.FirstOrDefault(it => it.Name == item.MappedFileName);
                if (selectedBlob == null) continue;

                using (var stream = selectedBlob.OpenRead())
                using (var reader = new StreamReader(stream))
                using (var jrdr = new JsonTextReader(reader))
                {
                    var gjt = JToken.ReadFrom(jrdr);
                    var ft = gjt.ToObject<GeoJSON.Net.Feature.Feature>();
                    //Console.WriteLine(ft);
                    var ctr = TurfCS.Turf.Center(ft);
                    //Console.WriteLine(ctr.Geometry.Type);
                    //var pt = (GeoJSON.Net.Geometry.Point)ctr.Geometry;
                    //Console.WriteLine(JsonConvert.SerializeObject(ctr));
                    //var coord = pt.Coordinates as GeoJSON.Net.Geometry.GeographicPosition;
                    //Console.WriteLine("{0}, {1}", coord.Latitude, coord.Longitude);
                    var geometry = ctr.Geometry as GeoJSON.Net.Geometry.Point;
                    var geographicPosition = geometry.Coordinates as GeoJSON.Net.Geometry.GeographicPosition;
                    var isDataValid = !double.IsNaN(geographicPosition.Longitude) && !double.IsNaN(geographicPosition.Latitude);
                    if (isDataValid)
                    {
                        updateEADic.Add(item.Id, new
                        {
                            coordinates = new[] { geographicPosition.Longitude, geographicPosition.Latitude },
                            type = ctr.Geometry.Type.ToString(),
                        });
                    }
                    else
                    {
                        using (var file = File.AppendText(LogFilePath))
                        {
                            file.WriteLine($"NaN found: {item.MappedFileName}");
                        }
                    }
                }

                updateProgress(++calculationRound, eaQry.Count(), "calculation");
            }
            #endregion Calculate center

            #region Update ea collection
            var updateRound = 0;
            foreach (var item in updateEADic)
            {
                var filter = Builders<BsonDocument>.Filter.Eq("_id", item.Key);
                var update = Builders<BsonDocument>.Update.Set("Center", item.Value);
                collection.UpdateOne(filter, update);

                updateProgress(++updateRound, eaQry.Count(), "update-db");
            }
            #endregion Update ea collection

            #region Update progression
            taks[rp.Id] = true;
            Console.WriteLine($"{rp.Name} - DONE, completed: ({taks.Count(it => it)}/{taks.Count()})");
            if (taks.All(it => it))
            {
                Console.WriteLine("---> ALL DONE!!");
                using (var file = File.AppendText(LogFilePath))
                {
                    file.WriteLine($"End: {DateTime.Now}");
                }
            }

            void updateProgress(double completed, int total, string msg)
            {
                if (completed % 100 == 0)
                {
                    var percent = completed / total * 100;
                    Console.WriteLine($"{rp.Name}, {msg} - {(int)percent}%");
                }
            }
            #endregion Update progression
        }
    }
}
