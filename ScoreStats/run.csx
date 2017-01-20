#r "Newtonsoft.Json"
#r "Microsoft.WindowsAzure.Storage"
using System;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;

public static async Task Run(RawStat statToScore, CloudTable scoredStats, TraceWriter log)
{
    log.Info($"Processing stat: {statToScore}");

    var stat = new ScoredStat(statToScore);
    log.Info($"Writing scored stat: {stat}");

    var upsert = TableOperation.InsertOrReplace(stat);
    await scoredStats.ExecuteAsync(upsert);
}

public class ScoredStat : TableEntity
{
    public ScoredStat()
    {
    }

    public ScoredStat(RawStat stat)
    {
        PartitionKey = stat.Round.ToString();
        RowKey = stat.PlayerId.ToString();
        Forward = stat.Goals*6 + stat.Behinds;
        Midfield = stat.Disposals;
        Ruck = stat.Marks + stat.Hitouts;
        Tackle = stat.Tackles*6;
    }

    [JsonProperty("f")]
    public int Forward { get; set; }
    [JsonProperty("m")]
    public int Midfield { get; set; }
    [JsonProperty("r")]
    public int Ruck { get; set; }
    [JsonProperty("t")]
    public int Tackle { get; set; }

    public static implicit operator string(ScoredStat stat)
    {
        return stat.ToString();
    }

    public override string ToString()
    {
        return JsonConvert.SerializeObject(this);
    }
}

public struct RawStat
{
    public Guid PlayerId { get; set; }
    public int Round { get; set; }
    public int Goals { get; set; }
    public int Behinds { get; set; }
    public int Disposals { get; set; }
    public int Marks { get; set; }
    public int Hitouts { get; set; }
    public int Tackles { get; set; }

    public static implicit operator string(RawStat stat)
    {
        return stat.ToString();
    }

    public override string ToString()
    {
        return JsonConvert.SerializeObject(this);
    }
}