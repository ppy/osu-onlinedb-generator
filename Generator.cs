using Microsoft.Data.Sqlite;
using MySqlConnector;
using SharpCompress.Compressors.BZip2;
using System;
using System.Data;
using System.IO;
using Dapper;

namespace osu.Server.OnlineDbGenerator
{
    public class Generator
    {
        /// <summary>
        /// SQL conditional for a beatmap to be stored in the online.db cache file.
        /// </summary>
        private const string sql_conditional = "WHERE approved > 0 AND deleted_at IS NULL";

        /// <summary>
        /// Online database to fetch beatmaps from.
        /// </summary>
        private readonly MySqlConnection mysql = getMySqlConnection();

        /// <summary>
        /// Path to the output online.db cache file.
        /// </summary>
        private static string sqliteFilePath => Environment.GetEnvironmentVariable("SQLITE_PATH") ?? "sqlite/online.db";

        private readonly SqliteConnection sqlite = getSqliteConnection();

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
            Console.WriteLine("Starting generator...");
            createSchema();
            Console.WriteLine("Created schema.");
            copyBeatmaps();

            mysql.Close();
            sqlite.Close();

            if (compressSqliteBz2)
            {
                Console.WriteLine("Compressing...");
                using (var inStream = File.OpenRead(sqliteFilePath))
                using (var outStream = File.OpenWrite(sqliteBz2FilePath))
                using (var bz2 = new BZip2Stream(outStream, SharpCompress.Compressors.CompressionMode.Compress, false))
                    inStream.CopyTo(bz2);
            }

            Console.WriteLine("All done!");
        }

        /// <summary>
        /// Create the schema inside the online.db SQLite database.
        /// </summary>
        private void createSchema()
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
        private void copyBeatmaps()
        {
            int total = countBeatmaps(mysql);
            Console.WriteLine($"Copying {total} beatmaps...");
            var start = DateTime.Now;

            var selectBeatmapsReader = selectBeatmaps(mysql);
            insertBeatmaps(sqlite, selectBeatmapsReader);

            var timespan = (DateTime.Now - start).TotalMilliseconds;
            Console.WriteLine($"Copied all beatmaps in {timespan}ms!");
        }

        /// <summary>
        /// Fetch beatmaps from MySQL or SQLite database, with offset and limit options.
        /// </summary>
        /// <param name="conn">Connection to fetch beatmaps from.</param>
        private IDataReader selectBeatmaps(IDbConnection conn)
        {
            return conn.ExecuteReader($"SELECT * FROM osu_beatmaps2 {sql_conditional} LIMIT 2000");
        }

        /// <summary>
        /// Insert beatmaps into the SQLite database.
        /// </summary>
        /// <param name="conn">Connection to insert beatmaps into.</param>
        /// <param name="beatmaps">DbDataReader object (obtained from SelectBeatmaps) to insert beatmaps from.</param>
        private void insertBeatmaps(SqliteConnection conn, IDataReader beatmaps)
        {
            const string sql = "INSERT INTO osu_beatmaps VALUES(@beatmap_id, @beatmapset_id, @user_id, @filename, @checksum, @version, @total_length, @hit_length, @countTotal, @countNormal, @countSlider, @countSpinner, @diff_drain, @diff_size, @diff_overall, @diff_approach, @playmode, @approved, @last_update, @difficultyrating, @playcount, @passcount, @orphaned, @youtube_preview, @score_version, @deleted_at, @bpm)";

            int processedItems = 0;

            while (beatmaps.Read())
            {
                conn.Execute(sql, new
                {
                    beatmap_id = beatmaps.GetInt32(0),
                    beatmapset_id = beatmaps.GetInt32(1),
                    user_id = beatmaps.GetInt32(2),
                    filename = beatmaps.GetString(3),
                    checksum = beatmaps.GetString(4),
                    version = beatmaps.GetString(5),
                    total_length = beatmaps.GetInt32(6),
                    hit_length = beatmaps.GetInt32(7),
                    countTotal = beatmaps.GetInt32(8),
                    countNormal = beatmaps.GetInt32(9),
                    countSlider = beatmaps.GetInt32(10),
                    countSpinner = beatmaps.GetInt32(11),
                    diff_drain = beatmaps.GetDecimal(12),
                    diff_size = beatmaps.GetDecimal(13),
                    diff_overall = beatmaps.GetDecimal(14),
                    diff_approach = beatmaps.GetDecimal(15),
                    playmode = beatmaps.GetInt32(16),
                    approved = beatmaps.GetInt32(17),
                    last_update = beatmaps.IsDBNull(18) ? (object)DBNull.Value : beatmaps.GetDateTime(18).ToString("yyyy-MM-dd HH:mm:ss"),
                    difficultyrating = beatmaps.GetDecimal(19),
                    playcount = beatmaps.GetInt32(20),
                    passcount = beatmaps.GetInt32(21),
                    orphaned = beatmaps.GetInt32(22),
                    youtube_preview = beatmaps.IsDBNull(23) ? (object)DBNull.Value : beatmaps.GetString(23),
                    score_version = beatmaps.GetInt32(24),
                    deleted_at = beatmaps.IsDBNull(25) ? (object)DBNull.Value : beatmaps.GetDateTime(25).ToString("yyyy-MM-dd HH:mm:ss"),
                    bpm = beatmaps.GetDecimal(26)
                });

                if (++processedItems % 50 == 0)
                    Console.WriteLine($"Copied {processedItems} beatmaps...");
            }

            beatmaps.Close();
        }

        /// <summary>
        /// Count beatmaps from MySQL or SQLite database.
        /// </summary>
        /// <param name="conn">Connection to fetch beatmaps from.</param>
        private int countBeatmaps(IDbConnection conn)
        {
            return conn.QuerySingle<int>($"SELECT COUNT(beatmap_id) FROM osu_beatmaps2 {sql_conditional}");
        }

        /// <summary>
        /// Get a connection to the offline SQLite cache database.
        /// </summary>
        /// <param name="erase">Whether to start fresh.</param>
        private static SqliteConnection getSqliteConnection(bool erase = true)
        {
            if (erase && File.Exists(sqliteFilePath))
                File.Delete(sqliteFilePath);

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
