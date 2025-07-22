using System.Net.Http;
using System;
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
        private const string beatmap_filter_conditions = "approved IN (1, 2, 4)";

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
                copyTags(mysql, sqlite);
                copyBeatmapTags(mysql, sqlite);
                copyUsernames(mysql, sqlite);
                copyBeatmapOwners(mysql, sqlite);

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
            sqlite.Execute("INSERT INTO `schema_version` (`number`) VALUES (3)");

            sqlite.Execute(
                """
                CREATE TABLE `osu_beatmapsets` (
                    `beatmapset_id` mediumint unsigned NOT NULL,
                    `submit_date` timestamp NOT NULL DEFAULT NULL,
                    `approved_date` timestamp NULL DEFAULT NULL,
                    `approved` timestamp NULL DEFAULT NULL,
                    PRIMARY KEY (`beatmapset_id`))
                """);

            sqlite.Execute(
                """
                CREATE TABLE `osu_beatmaps` (
                    `beatmap_id` mediumint unsigned NOT NULL,
                    `beatmapset_id` mediumint unsigned DEFAULT NULL,
                    `user_id` int unsigned NOT NULL DEFAULT '0',
                    `filename` varchar(150) DEFAULT NULL,
                    `checksum` varchar(32) DEFAULT NULL,
                    `approved` tinyint NOT NULL DEFAULT '0',
                    `last_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (`beatmap_id`))
                """);

            sqlite.Execute("CREATE INDEX `beatmapset_id` ON osu_beatmaps (`beatmapset_id`)");
            sqlite.Execute("CREATE INDEX `filename` ON osu_beatmaps (`filename`)");
            sqlite.Execute("CREATE INDEX `checksum` ON osu_beatmaps (`checksum`)");
            sqlite.Execute("CREATE INDEX `user_id` ON osu_beatmaps (`user_id`)");

            sqlite.Execute(
                """
                CREATE TABLE `tags` (
                    `id` bigint unsigned NOT NULL,
                    `name` varchar(255) DEFAULT NULL,
                    PRIMARY KEY (`id`))
                """);

            sqlite.Execute(
                """
                CREATE TABLE `beatmap_tags` (
                    `beatmap_id` int unsigned NOT NULL,
                    `tag_id` int unsigned NOT NULL,
                    PRIMARY KEY (`beatmap_id`, `tag_id`))
                """);

            sqlite.Execute(
                """
                CREATE TABLE `phpbb_users` (
                    `user_id` int unsigned NOT NULL,
                    `username` varchar(255) DEFAULT NULL,
                    PRIMARY KEY (`user_id`))
                """);

            sqlite.Execute(
                """
                CREATE TABLE `beatmap_owners` (
                    `beatmap_id` mediumint unsigned NOT NULL,
                    `user_id` int unsigned NOT NULL,
                    PRIMARY KEY (`beatmap_id`, `user_id`))
                """);
        }

        /// <summary>
        /// Copy all beatmaps from online MySQL database to cache SQLite database.
        /// </summary>
        private void copyBeatmapSets(IDbConnection source, IDbConnection destination)
        {
            int sourceCount = source.QuerySingle<int>($"SELECT COUNT(beatmapset_id) FROM osu_beatmapsets WHERE {beatmap_filter_conditions}");
            Console.WriteLine($"Copying {sourceCount} beatmap sets...");

            var start = DateTime.Now;
            int processedItems = 0;

            // only include "permanent" states – ranked, approved, loved.
            // this cache may be preferred for initial metadata fetches in lazer so we don't want to include any beatmaps which are still shifting in state.
            var sourceBeatmapSets = source.Query<BeatmapSetRow>($"SELECT beatmapset_id, approved, approved_date, submit_date FROM osu_beatmapsets WHERE {beatmap_filter_conditions}");

            foreach (var beatmapset in sourceBeatmapSets)
            {
                destination.Execute("INSERT INTO osu_beatmapsets VALUES(@beatmapset_id, @submit_date, @approved_date, @approved)", beatmapset);

                if (++processedItems % 50 == 0)
                    Console.WriteLine($"Copied {processedItems} beatmap sets...");
            }

            var timespan = (DateTime.Now - start).TotalMilliseconds;
            int destinationCount = destination.QuerySingle<int>("SELECT COUNT(beatmapset_id) FROM osu_beatmapsets");

            Console.WriteLine($"Copied beatmap sets in {timespan}ms! (mysql:{sourceCount} sqlite:{destinationCount})");

            if (destinationCount != sourceCount)
                throw new Exception($"Expected {sourceCount} beatmap sets, but found {destinationCount} in sqlite! Aborting");
        }

        /// <summary>
        /// Copy all beatmaps from online MySQL database to cache SQLite database.
        /// </summary>
        private void copyBeatmaps(IDbConnection source, IDbConnection destination)
        {
            int sourceCount = source.QuerySingle<int>($"SELECT COUNT(beatmap_id) FROM osu_beatmaps WHERE {beatmap_filter_conditions}");
            Console.WriteLine($"Copying {sourceCount} beatmaps...");

            var start = DateTime.Now;
            int processedItems = 0;

            var sourceBeatmaps = source.Query<BeatmapRow>($"SELECT beatmap_id, beatmapset_id, user_id, filename, checksum, approved, last_update FROM osu_beatmaps WHERE {beatmap_filter_conditions}");

            foreach (var beatmap in sourceBeatmaps)
            {
                destination.Execute("INSERT INTO osu_beatmaps VALUES(@beatmap_id, @beatmapset_id, @user_id, @filename, @checksum, @approved, @last_update)", beatmap);

                if (++processedItems % 50 == 0)
                    Console.WriteLine($"Copied {processedItems} beatmaps...");
            }

            var timespan = (DateTime.Now - start).TotalMilliseconds;
            int destinationCount = destination.QuerySingle<int>($"SELECT COUNT(beatmap_id) FROM osu_beatmaps");

            Console.WriteLine($"Copied beatmaps in {timespan}ms! (mysql:{sourceCount} sqlite:{destinationCount})");

            if (destinationCount != sourceCount)
                throw new Exception($"Expected {sourceCount} beatmaps, but found {destinationCount} in sqlite! Aborting");
        }

        private void copyTags(IDbConnection source, IDbConnection destination)
        {
            int sourceCount = source.QuerySingle<int>("SELECT COUNT(`id`) FROM `tags`");
            Console.WriteLine($"Copying {sourceCount} tags...");

            var start = DateTime.Now;
            int processedItems = 0;

            var sourceTags = source.Query<TagRow>("SELECT `id`, `name` FROM `tags`");

            foreach (var tag in sourceTags)
            {
                destination.Execute("INSERT INTO `tags` VALUES(@id, @name)", tag);

                if (++processedItems % 50 == 0)
                    Console.WriteLine($"Copied {processedItems} tags...");
            }

            var timespan = (DateTime.Now - start).TotalMilliseconds;
            int destinationCount = destination.QuerySingle<int>("SELECT COUNT(`id`) FROM tags");

            Console.WriteLine($"Copied tags in {timespan}ms! (mysql:{sourceCount} sqlite:{destinationCount})");

            if (destinationCount != sourceCount)
                throw new Exception($"Expected {sourceCount} tags, but found {destinationCount} in sqlite! Aborting");
        }

        private void copyBeatmapTags(IDbConnection source, IDbConnection destination)
        {
            int sourceCount = source.QuerySingle<int>("SELECT COUNT(DISTINCT `beatmap_id`, `tag_id`) FROM `beatmap_tags`");
            Console.WriteLine($"Copying {sourceCount} beatmap tag pairs...");

            var start = DateTime.Now;
            int processedItems = 0;

            var sourceBeatmapTags = source.Query<BeatmapTagRow>("SELECT DISTINCT `beatmap_id`, `tag_id` FROM `beatmap_tags`");

            foreach (var beatmapTag in sourceBeatmapTags)
            {
                destination.Execute("INSERT INTO `beatmap_tags` VALUES(@beatmap_id, @tag_id)", beatmapTag);

                if (++processedItems % 50 == 0)
                    Console.WriteLine($"Copied {processedItems} tags...");
            }

            var timespan = (DateTime.Now - start).TotalMilliseconds;
            int destinationCount = destination.QuerySingle<int>("SELECT COUNT(1) FROM `beatmap_tags`");

            Console.WriteLine($"Copied beatmap tags in {timespan}ms! (mysql:{sourceCount} sqlite:{destinationCount})");

            if (destinationCount != sourceCount)
                throw new Exception($"Expected {sourceCount} beatmap tags, but found {destinationCount} in sqlite! Aborting");
        }

        private void copyUsernames(IDbConnection source, IDbConnection destination)
        {
            int sourceCount = source.QuerySingle<int>("SELECT COUNT(`user_id`) FROM `phpbb_users`");
            Console.WriteLine($"Copying {sourceCount} usernames...");

            var start = DateTime.Now;
            int processedItems = 0;

            var sourceUsers = source.Query<UserRow>("SELECT `user_id`, `username` FROM `phpbb_users`");

            foreach (var user in sourceUsers)
            {
                destination.Execute("INSERT INTO `phpbb_users` VALUES(@user_id, @username)", user);

                if (++processedItems % 50 == 0)
                    Console.WriteLine($"Copied {processedItems} usernames...");
            }

            var timespan = (DateTime.Now - start).TotalMilliseconds;
            int destinationCount = destination.QuerySingle<int>("SELECT COUNT(`user_id`) FROM `phpbb_users`");

            Console.WriteLine($"Copied usernames in {timespan}ms! (mysql:{sourceCount} sqlite:{destinationCount})");

            if (destinationCount != sourceCount)
                throw new Exception($"Expected {sourceCount} usernames, but found {destinationCount} in sqlite! Aborting");
        }

        private void copyBeatmapOwners(IDbConnection source, IDbConnection destination)
        {
            int sourceCount = source.QuerySingle<int>("SELECT COUNT(1) FROM `beatmap_owners`");
            Console.WriteLine($"Copying {sourceCount} beatmap owners...");

            var start = DateTime.Now;
            int processedItems = 0;

            var sourceBeatmapOwners = source.Query<BeatmapOwnerRow>("SELECT `beatmap_id`, `user_id` FROM `beatmap_owners`");

            foreach (var owner in sourceBeatmapOwners)
            {
                destination.Execute("INSERT INTO `beatmap_owners` VALUES(@beatmap_id, @user_id)", owner);

                if (++processedItems % 50 == 0)
                    Console.WriteLine($"Copied {processedItems} beatmap owners...");
            }

            var timespan = (DateTime.Now - start).TotalMilliseconds;
            int destinationCount = destination.QuerySingle<int>("SELECT COUNT(1) FROM `beatmap_owners`");

            Console.WriteLine($"Copied beatmap owners in {timespan}ms! (mysql:{sourceCount} sqlite:{destinationCount})");

            if (destinationCount != sourceCount)
                throw new Exception($"Expected {sourceCount} beatmap owners, but found {destinationCount} in sqlite! Aborting");
        }

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
