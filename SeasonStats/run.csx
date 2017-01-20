#r "Newtonsoft.Json"
#r "Microsoft.WindowsAzure.Storage"
using System;
using System.Collections;
using System.Text.RegularExpressions;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;

public static void Run(ScoredStat scoredStat, CloudTable seasonStatsByPlayer, TraceWriter log)
{
    log.Info($"Processing stat {scoredStat.ToString()}");

    var query = TableOperation.Retrieve<SeasonStats>(scoredStat.Round.Year.ToString(), scoredStat.RowKey);
    var result = seasonStatsByPlayer.Execute(query);
    
    var original = (SeasonStats)(result.Result ?? new SeasonStats { Year = scoredStat.Round.Year, RowKey = scoredStat.RowKey });
    bool newEntity = result.Result == null;

    if (original.Stats.ContainsKey(scoredStat.Round.RoundNumber))
    {
        original.Stats[scoredStat.Round.RoundNumber] = scoredStat.Stat;
    }
    else
    {
        original.Stats.Add(scoredStat.Round.RoundNumber, scoredStat.Stat);
    }

    if (newEntity)
    {
        TableOperation insertOperation = TableOperation.Insert(original);
        seasonStatsByPlayer.Execute(insertOperation);
    }
    else
    {
        TableOperation updateOperation = TableOperation.Replace(original);
        seasonStatsByPlayer.Execute(updateOperation);
    }
}

public struct Round
{
    public int RoundId { get; set; }
    public int Year { get; set; }
    public int RoundNumber { get; set; }

    public static Regex roundRegex = new Regex(@"^\d{6}$", RegexOptions.Compiled);

    public static implicit operator Round(string roundId)
    {
        if (!roundRegex.IsMatch(roundId))
        {
            throw new ArgumentOutOfRangeException(nameof(roundId), roundId, "Round Id should be 6 digits exactly (year[4])(round[2])");
        }

        return new Round
        {
            Year = int.Parse(roundId.Substring(0, 4)),
            RoundNumber = int.Parse(roundId.Substring(4, 2)),
            RoundId = int.Parse(roundId)
        };
    }

    public static implicit operator string(Round round)
    {
        return round.RoundId.ToString();
    }
}

public class ScoredStat : TableEntity
{
    [JsonIgnore]
    public Round Round => (Round) PartitionKey;

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

    [JsonIgnore]
    public Stat Stat => new Stat
        {
            Round = Round,
            Forward = Forward,
            Midfield = Midfield,
            Ruck = Ruck,
            Tackle = Tackle
        };
}

public class Stat
{
    public Round Round { get; set; }
    [JsonProperty("f")]
    public int Forward { get; set; }
    [JsonProperty("m")]
    public int Midfield { get; set; }
    [JsonProperty("r")]
    public int Ruck { get; set; }
    [JsonProperty("t")]
    public int Tackle { get; set; }
}

public class SeasonStats : TableEntity
{
    public SeasonStats()
    {
        Stats = new Dictionary<int,Stat>();
    }

    [JsonIgnore]
    public int Year {
        get { return int.Parse(PartitionKey); }
        set { PartitionKey = value.ToString(); }
    }

    public Guid PlayerId
    {
        get { return Guid.Parse(RowKey); }
        set { RowKey = value.ToString(); }
    }
    [JsonIgnore]
    public Dictionary<int, Stat> Stats { get; set; }
    [JsonProperty("Stats")]
    public string StatsString
    {
        get {return JsonConvert.SerializeObject(Stats);}
        set {Stats = JsonConvert.DeserializeObject<Dictionary<int,Stat>>(value);}
    }
}