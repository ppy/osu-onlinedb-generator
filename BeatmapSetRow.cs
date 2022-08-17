// Copyright (c) ppy Pty Ltd <contact@ppy.sh>. Licensed under the MIT Licence.
// See the LICENCE file in the repository root for full licence text.

using System;

// ReSharper disable InconsistentNaming (intentionally matching database naming)

namespace osu.Server.OnlineDbGenerator
{
    public class BeatmapSetRow
    {
        public int beatmapset_id { get; set; }
        public int approved { get; set; }
        public DateTimeOffset approved_date { get; set; }
        public DateTimeOffset submit_date { get; set; }
    }
}
