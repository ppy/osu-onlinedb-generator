using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
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
        /// Whether to compress the online.db file to bz2.
        /// </summary>
        private static bool compressSqliteBz2 => true;

        /// <summary>
        /// Path to the bz2-compressed online.db cache file.
        /// </summary>
        private static string sqliteBz2FilePath => $"{sqliteFilePath}.bz2";

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
                copyBeatmaps(mysql, sqlite);

                if (compressSqliteBz2)
                {
                    Console.WriteLine("Compressing...");

                    using (var inStream = File.OpenRead(sqliteFilePath))
                    using (var outStream = File.OpenWrite(sqliteBz2FilePath))
                    using (var bz2 = new BZip2Stream(outStream, CompressionMode.Compress, false))
                        inStream.CopyTo(bz2);
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
            sqlite.Execute(@"CREATE TABLE `osu_beatmaps` (
                                  `beatmap_id` mediumint unsigned NOT NULL,
                                  `beatmapset_id` mediumint unsigned DEFAULT NULL,
                                  `user_id` int unsigned NOT NULL DEFAULT '0',
                                  `filename` varchar(150) DEFAULT NULL,
                                  `checksum` varchar(32) DEFAULT NULL,
                                  `version` varchar(80) NOT NULL DEFAULT '',
                                  `total_length` mediumint unsigned NOT NULL DEFAULT '0',
                                  `hit_length` mediumint unsigned NOT NULL DEFAULT '0',
                                  `countTotal` smallint unsigned NOT NULL DEFAULT '0',
                                  `countNormal` smallint unsigned NOT NULL DEFAULT '0',
                                  `countSlider` smallint unsigned NOT NULL DEFAULT '0',
                                  `countSpinner` smallint unsigned NOT NULL DEFAULT '0',
                                  `diff_drain` float unsigned NOT NULL DEFAULT '0',
                                  `diff_size` float unsigned NOT NULL DEFAULT '0',
                                  `diff_overall` float unsigned NOT NULL DEFAULT '0',
                                  `diff_approach` float unsigned NOT NULL DEFAULT '0',
                                  `playmode` tinyint unsigned NOT NULL DEFAULT '0',
                                  `approved` tinyint NOT NULL DEFAULT '0',
                                  `last_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                  `difficultyrating` float NOT NULL DEFAULT '0',
                                  `playcount` int unsigned NOT NULL DEFAULT '0',
                                  `passcount` int unsigned NOT NULL DEFAULT '0',
                                  `orphaned` tinyint(1) NOT NULL DEFAULT '0',
                                  `youtube_preview` varchar(50) DEFAULT NULL,
                                  `score_version` tinyint NOT NULL DEFAULT '1',
                                  `deleted_at` timestamp NULL DEFAULT NULL,
                                  `bpm` float DEFAULT NULL,
                                  PRIMARY KEY (`beatmap_id`))");

            sqlite.Execute("CREATE INDEX `beatmapset_id` ON osu_beatmaps (`beatmapset_id`)");
            sqlite.Execute("CREATE INDEX `filename` ON osu_beatmaps (`filename`)");
            sqlite.Execute("CREATE INDEX `checksum` ON osu_beatmaps (`checksum`)");
            sqlite.Execute("CREATE INDEX `user_id` ON osu_beatmaps (`user_id`)");
        }

        /// <summary>
        /// Copy all beatmaps from online MySQL database to cache SQLite database.
        /// </summary>
        private void copyBeatmaps(IDbConnection source, IDbConnection destination)
        {
            int total = getBeatmapCount(source);
            Console.WriteLine($"Copying {total} beatmaps...");

            var start = DateTime.Now;

            var beatmapsReader = source.Query<BeatmapRow>("SELECT * FROM osu_beatmaps WHERE approved > 0 AND deleted_at IS NULL");

            insertBeatmaps(destination, beatmapsReader);

            var timespan = (DateTime.Now - start).TotalMilliseconds;

            int totalSqlite = getBeatmapCount(destination);

            Console.WriteLine($"Copied beatmaps in {timespan}ms! (mysql:{total} sqlite:{totalSqlite})");
        }

        /// <summary>
        /// Insert beatmaps into the SQLite database.
        /// </summary>
        /// <param name="conn">Connection to insert beatmaps into.</param>
        /// <param name="beatmaps">DbDataReader object (obtained from SelectBeatmaps) to insert beatmaps from.</param>
        private void insertBeatmaps(IDbConnection conn, IEnumerable<BeatmapRow> beatmaps)
        {
            const string sql = "INSERT INTO osu_beatmaps VALUES(@beatmap_id, @beatmapset_id, @user_id, @filename, @checksum, @version, @total_length, @hit_length, @countTotal, @countNormal, @countSlider, @countSpinner, @diff_drain, @diff_size, @diff_overall, @diff_approach, @playmode, @approved, @last_update, @difficultyrating, @playcount, @passcount, @orphaned, @youtube_preview, @score_version, @deleted_at, @bpm)";

            int processedItems = 0;

            foreach (var beatmap in beatmaps)
            {
                conn.Execute(sql, beatmap);

                if (++processedItems % 50 == 0)
                    Console.WriteLine($"Copied {processedItems} beatmaps...");
            }
        }

        /// <summary>
        /// Count beatmaps from MySQL or SQLite database.
        /// </summary>
        /// <param name="conn">Connection to fetch beatmaps from.</param>
        private int getBeatmapCount(IDbConnection conn) => conn.QuerySingle<int>("SELECT COUNT(beatmap_id) FROM osu_beatmaps WHERE approved > 0 AND deleted_at IS NULL");

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
    }
}
