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
        public int approved { get; set; }
        public DateTimeOffset last_update { get; set; }
    }
}
