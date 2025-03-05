using System.Net.Http;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Dapper;
using Microsoft.Data.Sqlite;
using MySqlConnector;
using SharpCompress.Compressors;
using SharpCompress.Compressors.BZip2;

namespace osu.Server.OnlineDbGenerator
{
    public class Generator
    {
        /// <summary>
        /// Path to the output online.db cache file.
        /// </summary>
        private static string sqliteFilePath => Environment.GetEnvironmentVariable("SQLITE_PATH") ?? "sqlite/online.db";

        /// <summary>
        /// Path to the bz2-compressed online.db cache file.
        /// </summary>
        private static string sqliteBz2FilePath => $"{sqliteFilePath}.bz2";

        /// <summary>
        /// Conditional to filter beatmaps and beatmapsets by.
        /// </summary>
        private const string where_conditions = "approved IN (1, 2, 4)";

        /// <summary>
        /// Start generating the online.db file.
        /// </summary>
        public void Run()
        {
            using (var sqlite = getSqliteConnection())
            using (var mysql = getMySqlConnection())
            {
                Console.WriteLine("Starting generator...");

                createSchema(sqlite);
                Console.WriteLine("Created schema.");

                copyBeatmapSets(mysql, sqlite);
                copyBeatmaps(mysql, sqlite);

                Console.WriteLine("Compressing...");

                using (var inStream = File.OpenRead(sqliteFilePath))
                using (var outStream = File.OpenWrite(sqliteBz2FilePath))
                using (var bz2 = new BZip2Stream(outStream, CompressionMode.Compress, false))
                    inStream.CopyTo(bz2);

                if (Environment.GetEnvironmentVariable("S3_KEY") != null)
                {
                    Console.WriteLine("Uploading to S3...");

                    using (var stream = File.OpenRead(sqliteBz2FilePath))
                        Upload("assets.ppy.sh", "client-resources/online.db.bz2", stream, stream.Length, "application/x-bzip2");

                    if (Environment.GetEnvironmentVariable("S3_PROXY_CACHE_PURGE_KEY") != null)
                    {
                        Console.WriteLine("Purging s3-nginx-proxy cache...");
                        PurgeCache("https://assets.ppy.sh/client-resources/online.db.bz2", Environment.GetEnvironmentVariable("S3_PROXY_CACHE_PURGE_KEY"));
                    }
                }
            }

            Console.WriteLine("All done!");
        }

        /// <summary>
        /// Create the schema inside the online.db SQLite database.
        /// </summary>
        /// <param name="sqlite"></param>
        private void createSchema(SqliteConnection sqlite)
        {
            sqlite.Execute("CREATE TABLE `schema_version` (`number` smallint unsigned NOT NULL)");
            sqlite.Execute("INSERT INTO `schema_version` (`number`) VALUES (2)");

            sqlite.Execute(@"CREATE TABLE `osu_beatmapsets` (
                                  `beatmapset_id` mediumint unsigned NOT NULL,
                                  `submit_date` timestamp NOT NULL DEFAULT NULL,
                                  `approved_date` timestamp NULL DEFAULT NULL,
                                  `approved` timestamp NULL DEFAULT NULL,
                                  PRIMARY KEY (`beatmapset_id`))");

            sqlite.Execute(@"CREATE TABLE `osu_beatmaps` (
                                  `beatmap_id` mediumint unsigned NOT NULL,
                                  `beatmapset_id` mediumint unsigned DEFAULT NULL,
                                  `user_id` int unsigned NOT NULL DEFAULT '0',
                                  `filename` varchar(150) DEFAULT NULL,
                                  `checksum` varchar(32) DEFAULT NULL,
                                  `approved` tinyint NOT NULL DEFAULT '0',
                                  `last_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                  PRIMARY KEY (`beatmap_id`))");

            sqlite.Execute("CREATE INDEX `beatmapset_id` ON osu_beatmaps (`beatmapset_id`)");
            sqlite.Execute("CREATE INDEX `filename` ON osu_beatmaps (`filename`)");
            sqlite.Execute("CREATE INDEX `checksum` ON osu_beatmaps (`checksum`)");
            sqlite.Execute("CREATE INDEX `user_id` ON osu_beatmaps (`user_id`)");
        }

        /// <summary>
        /// Copy all beatmaps from online MySQL database to cache SQLite database.
        /// </summary>
        private void copyBeatmapSets(IDbConnection source, IDbConnection destination)
        {
            int total = getBeatmapSetCount(source);
            Console.WriteLine($"Copying {total} beatmap sets...");

            var start = DateTime.Now;

            // only include "permanent" states – ranked, approved, loved.
            // this cache may be preferred for initial metadata fetches in lazer so we don't want to include any beatmaps which are still shifting in state.
            var beatmapSetsReader = source.Query<BeatmapSetRow>($"SELECT beatmapset_id, approved, approved_date, submit_date FROM osu_beatmapsets WHERE {where_conditions}");

            insertBeatmapSets(destination, beatmapSetsReader);

            var timespan = (DateTime.Now - start).TotalMilliseconds;

            int totalSqlite = getBeatmapSetCount(destination);

            Console.WriteLine($"Copied beatmap sets in {timespan}ms! (mysql:{total} sqlite:{totalSqlite})");

            if (totalSqlite != total)
            {
                throw new Exception($"Expected {total} beatmap sets, but found {totalSqlite} in sqlite! Aborting");
            }
        }

        /// <summary>
        /// Copy all beatmaps from online MySQL database to cache SQLite database.
        /// </summary>
        private void copyBeatmaps(IDbConnection source, IDbConnection destination)
        {
            int total = getBeatmapCount(source);
            Console.WriteLine($"Copying {total} beatmaps...");

            var start = DateTime.Now;

            var beatmapsReader = source.Query<BeatmapRow>($"SELECT beatmap_id, beatmapset_id, user_id, filename, checksum, approved, last_update FROM osu_beatmaps WHERE {where_conditions}");

            insertBeatmaps(destination, beatmapsReader);

            var timespan = (DateTime.Now - start).TotalMilliseconds;

            int totalSqlite = getBeatmapCount(destination);

            Console.WriteLine($"Copied beatmaps in {timespan}ms! (mysql:{total} sqlite:{totalSqlite})");

            if (totalSqlite != total)
            {
                throw new Exception($"Expected {total} beatmaps, but found {totalSqlite} in sqlite! Aborting");
            }
        }

        /// <summary>
        /// Insert beatmap sets into the SQLite database.
        /// </summary>
        /// <param name="conn">Connection to insert beatmaps into.</param>
        /// <param name="beatmaps">DbDataReader object (obtained from SelectBeatmaps) to insert beatmaps from.</param>
        private void insertBeatmapSets(IDbConnection conn, IEnumerable<BeatmapSetRow> beatmapsets)
        {
            const string sql = "INSERT INTO osu_beatmapsets VALUES(@beatmapset_id, @submit_date, @approved_date, @approved)";

            int processedItems = 0;

            foreach (var beatmapset in beatmapsets)
            {
                conn.Execute(sql, beatmapset);

                if (++processedItems % 50 == 0)
                    Console.WriteLine($"Copied {processedItems} beatmap sets...");
            }
        }

        /// <summary>
        /// Insert beatmaps into the SQLite database.
        /// </summary>
        /// <param name="conn">Connection to insert beatmaps into.</param>
        /// <param name="beatmaps">DbDataReader object (obtained from SelectBeatmaps) to insert beatmaps from.</param>
        private void insertBeatmaps(IDbConnection conn, IEnumerable<BeatmapRow> beatmaps)
        {
            const string sql = "INSERT INTO osu_beatmaps VALUES(@beatmap_id, @beatmapset_id, @user_id, @filename, @checksum, @approved, @last_update)";

            int processedItems = 0;

            foreach (var beatmap in beatmaps)
            {
                conn.Execute(sql, beatmap);

                if (++processedItems % 50 == 0)
                    Console.WriteLine($"Copied {processedItems} beatmaps...");
            }
        }

        /// <summary>
        /// Count beatmap sets from MySQL or SQLite database.
        /// </summary>
        /// <param name="conn">Connection to fetch beatmaps from.</param>
        private int getBeatmapSetCount(IDbConnection conn) => conn.QuerySingle<int>($"SELECT COUNT(beatmapset_id) FROM osu_beatmapsets WHERE {where_conditions}");

        /// <summary>
        /// Count beatmaps from MySQL or SQLite database.
        /// </summary>
        /// <param name="conn">Connection to fetch beatmaps from.</param>
        private int getBeatmapCount(IDbConnection conn) => conn.QuerySingle<int>($"SELECT COUNT(beatmap_id) FROM osu_beatmaps WHERE {where_conditions}");

        /// <summary>
        /// Get a connection to the offline SQLite cache database.
        /// </summary>
        /// <param name="erase">Whether to start fresh.</param>
        private static SqliteConnection getSqliteConnection(bool erase = true)
        {
            if (erase && File.Exists(sqliteFilePath))
                File.Delete(sqliteFilePath);

            Directory.CreateDirectory(Path.GetDirectoryName(sqliteFilePath));

            var connection = new SqliteConnection($"Data Source={sqliteFilePath}");
            connection.Open();
            return connection;
        }

        /// <summary>
        /// Get a connection to the online MySQL database.
        /// </summary>
        private static MySqlConnection getMySqlConnection()
        {
            string host = (Environment.GetEnvironmentVariable("DB_HOST") ?? "localhost");
            string user = (Environment.GetEnvironmentVariable("DB_USER") ?? "root");

            var connection = new MySqlConnection($"Server={host};Database=osu;User ID={user};ConnectionTimeout=5;ConnectionReset=false;Pooling=true;");
            connection.Open();
            return connection;
        }

        private static AmazonS3Client getClient(RegionEndpoint endpoint = null)
        {
            string key = Environment.GetEnvironmentVariable("S3_KEY");
            string secret = Environment.GetEnvironmentVariable("S3_SECRET");

            return new AmazonS3Client(new BasicAWSCredentials(key, secret), new AmazonS3Config
            {
                CacheHttpClient = true,
                HttpClientCacheSize = 32,
                RegionEndpoint = endpoint ?? RegionEndpoint.USWest1,
                UseHttp = true,
                ForcePathStyle = true
            });
        }

        public static void Upload(string bucket, string key, Stream stream, long contentLength, string contentType = null)
        {
            using (var client = getClient())
            {
                client.PutObjectAsync(new PutObjectRequest
                {
                    BucketName = bucket,
                    Key = key,
                    CannedACL = S3CannedACL.PublicRead,
                    Headers =
                    {
                        ContentLength = contentLength,
                        ContentType = contentType,
                    },
                    InputStream = stream
                }).Wait();
            }
        }

        public static void PurgeCache(string url, string key)
        {
            var client = new HttpClient();
            client.DefaultRequestHeaders.Add("Authorization", key);

            var request = client.DeleteAsync(url);
            request.Wait();
            request.Result.EnsureSuccessStatusCode();
        }
    }
}
