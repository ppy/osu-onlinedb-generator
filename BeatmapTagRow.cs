// Copyright (c) ppy Pty Ltd <contact@ppy.sh>. Licensed under the MIT Licence.
// See the LICENCE file in the repository root for full licence text.

// ReSharper disable InconsistentNaming (intentionally matching database naming)

namespace osu.Server.OnlineDbGenerator
{
    public class BeatmapTagRow
    {
        public int beatmap_id { get; set; }
        public int tag_id { get; set; }
    }
}
