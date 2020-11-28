using Microsoft.Data.Sqlite;
using MySqlConnector;
using SharpCompress.Compressors.BZip2;
using System;
using System.Data.Common;
using System.IO;

namespace osu.Server.OnlineDbGenerator
{
    public class Generator
    {
        /// <summary>
        /// SQL conditional for a beatmap to be stored in the online.db cache file.
        /// </summary>
        private const string sql_conditional = "WHERE approved > 0 AND deleted_at IS NULL";

        /// <summary>
        /// Amount of beatmaps to fetch and insert in a single operation.
        /// </summary>
        private const int step = 18;

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
            string[] commands = new string[5];
            commands[0] = @"CREATE TABLE `osu_beatmaps` (
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
  PRIMARY KEY (`beatmap_id`)
)";
            commands[1] = "CREATE INDEX `beatmapset_id` ON osu_beatmaps (`beatmapset_id`)";
            commands[2] = "CREATE INDEX `filename` ON osu_beatmaps (`filename`)";
            commands[3] = "CREATE INDEX `checksum` ON osu_beatmaps (`checksum`)";
            commands[4] = "CREATE INDEX `user_id` ON osu_beatmaps (`user_id`)";

            foreach (var sql in commands)
            {
                var command = sqlite.CreateCommand();
                command.CommandText = sql;
                command.ExecuteNonQuery();
            }
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

            // Insert {step} beatmaps at a time.
            for (int offset = 0; offset < total; offset += step)
            {
                var limit = Math.Min(step, total - offset);
                insertBeatmaps(sqlite, selectBeatmapsReader, limit);
                Console.WriteLine($"Copied {offset + limit} out of {total} beatmaps...");
            }

            var timespan = (DateTime.Now - start).TotalMilliseconds;
            Console.WriteLine($"Copied all beatmaps in {timespan}ms!");
            selectBeatmapsReader.Close();
        }

        /// <summary>
        /// Fetch beatmaps from MySQL or SQLite database, with offset and limit options.
        /// </summary>
        /// <param name="conn">Connection to fetch beatmaps from.</param>
        /// <param name="offset">Offset to fetch beatmaps from.</param>
        /// <param name="limit">Maximum amount of beatmaps we want to fetch.</param>
        private DbDataReader selectBeatmaps(DbConnection conn, int offset = -1, int limit = -1)
        {
            var command = conn.CreateCommand();
            command.CommandText = $"SELECT * FROM osu_beatmaps {sql_conditional}";
            if (limit > 0)
                command.CommandText += $" LIMIT {limit}";
            if (offset > 0)
                command.CommandText += $" OFFSET {offset}";
            return command.ExecuteReader();
        }

        /// <summary>
        /// Insert beatmaps into the SQLite database.
        /// </summary>
        /// <param name="conn">Connection to insert beatmaps into.</param>
        /// <param name="beatmaps">DbDataReader object (obtained from SelectBeatmaps) to insert beatmaps from.</param>
        /// <param name="limit">Amount of beatmaps to read</param>
        private int insertBeatmaps(SqliteConnection conn, DbDataReader beatmaps, int limit)
        {
            var command = conn.CreateCommand();
            command.CommandText = "INSERT INTO osu_beatmaps VALUES";
            int i = 0;

            while (i < limit && beatmaps.Read())
            {
                if (i > 0)
                    command.CommandText += ", ";
                command.CommandText += $"(@beatmap_id{i}, @beatmapset_id{i}, @user_id{i}, @filename{i}, @checksum{i}, @version{i}, @total_length{i}, @hit_length{i}, @countTotal{i}, @countNormal{i}, @countSlider{i}, @countSpinner{i}, @diff_drain{i}, @diff_size{i}, @diff_overall{i}, @diff_approach{i}, @playmode{i}, @approved{i}, @last_update{i}, @difficultyrating{i}, @playcount{i}, @passcount{i}, @orphaned{i}, @youtube_preview{i}, @score_version{i}, @deleted_at{i}, @bpm{i})";
                command.Prepare();
                command.Parameters.AddWithValue($"@beatmap_id{i}", beatmaps.GetInt32(0));
                command.Parameters.AddWithValue($"@beatmapset_id{i}", beatmaps.GetInt32(1));
                command.Parameters.AddWithValue($"@user_id{i}", beatmaps.GetInt32(2));
                command.Parameters.AddWithValue($"@filename{i}", beatmaps.GetString(3));
                command.Parameters.AddWithValue($"@checksum{i}", beatmaps.GetString(4));
                command.Parameters.AddWithValue($"@version{i}", beatmaps.GetString(5));
                command.Parameters.AddWithValue($"@total_length{i}", beatmaps.GetInt32(6));
                command.Parameters.AddWithValue($"@hit_length{i}", beatmaps.GetInt32(7));
                command.Parameters.AddWithValue($"@countTotal{i}", beatmaps.GetInt32(8));
                command.Parameters.AddWithValue($"@countNormal{i}", beatmaps.GetInt32(9));
                command.Parameters.AddWithValue($"@countSlider{i}", beatmaps.GetInt32(10));
                command.Parameters.AddWithValue($"@countSpinner{i}", beatmaps.GetInt32(11));
                command.Parameters.AddWithValue($"@diff_drain{i}", beatmaps.GetDecimal(12));
                command.Parameters.AddWithValue($"@diff_size{i}", beatmaps.GetDecimal(13));
                command.Parameters.AddWithValue($"@diff_overall{i}", beatmaps.GetDecimal(14));
                command.Parameters.AddWithValue($"@diff_approach{i}", beatmaps.GetDecimal(15));
                command.Parameters.AddWithValue($"@playmode{i}", beatmaps.GetInt32(16));
                command.Parameters.AddWithValue($"@approved{i}", beatmaps.GetInt32(17));
                command.Parameters.AddWithValue($"@last_update{i}", beatmaps.IsDBNull(18) ? (object)DBNull.Value : beatmaps.GetDateTime(18).ToString("yyyy-MM-dd HH:mm:ss"));
                command.Parameters.AddWithValue($"@difficultyrating{i}", beatmaps.GetDecimal(19));
                command.Parameters.AddWithValue($"@playcount{i}", beatmaps.GetInt32(20));
                command.Parameters.AddWithValue($"@passcount{i}", beatmaps.GetInt32(21));
                command.Parameters.AddWithValue($"@orphaned{i}", beatmaps.GetInt32(22));
                command.Parameters.AddWithValue($"@youtube_preview{i}", beatmaps.IsDBNull(23) ? (object)DBNull.Value : beatmaps.GetString(23));
                command.Parameters.AddWithValue($"@score_version{i}", beatmaps.GetInt32(24));
                command.Parameters.AddWithValue($"@deleted_at{i}", beatmaps.IsDBNull(25) ? (object)DBNull.Value : beatmaps.GetDateTime(25).ToString("yyyy-MM-dd HH:mm:ss"));
                command.Parameters.AddWithValue($"@bpm{i}", beatmaps.GetDecimal(26));
                i++;
            }

            return command.ExecuteNonQuery();
        }

        /// <summary>
        /// Count beatmaps from MySQL or SQLite database.
        /// </summary>
        /// <param name="conn">Connection to fetch beatmaps from.</param>
        private int countBeatmaps(DbConnection conn)
        {
            var command = conn.CreateCommand();
            command.CommandText = $"SELECT COUNT(beatmap_id) FROM osu_beatmaps {sql_conditional}";
            var reader = command.ExecuteReader();
            reader.Read();
            int count = reader.GetInt32(0);
            reader.Close();
            return count;
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
