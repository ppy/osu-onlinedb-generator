// Copyright (c) ppy Pty Ltd <contact@ppy.sh>. Licensed under the MIT Licence.
// See the LICENCE file in the repository root for full licence text.

using System;

// ReSharper disable InconsistentNaming (intentionally matching database naming)

namespace osu.Server.OnlineDbGenerator
{
    public class BeatmapRow
    {
        public int beatmap_id { get; set; }
        public int beatmapset_id { get; set; }
        public int user_id { get; set; }
        public string filename { get; set; }
        public string checksum { get; set; }
        public string version { get; set; }
        public int total_length { get; set; }
        public int hit_length { get; set; }
        public int countTotal { get; set; }
        public int countNormal { get; set; }
        public int countSlider { get; set; }
        public int countSpinner { get; set; }
        public double diff_drain { get; set; }
        public double diff_size { get; set; }
        public double diff_overall { get; set; }
        public double diff_approach { get; set; }
        public int playmode { get; set; }
        public int approved { get; set; }
        public DateTimeOffset last_update { get; set; }
        public double difficultyrating { get; set; }
        public int playcount { get; set; }
        public int passcount { get; set; }
        public int orphaned { get; set; }
        public string youtube_preview { get; set; }
        public int score_version { get; set; }
        public DateTimeOffset? deleted_at { get; set; }
        public double bpm { get; set; }
    }
}
