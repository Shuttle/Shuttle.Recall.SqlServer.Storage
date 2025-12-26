using Microsoft.EntityFrameworkCore;

namespace Shuttle.Recall.SqlServer.Storage.Models;

[PrimaryKey(nameof(Id), nameof(System.Version))]
public class PrimitiveEventJournal
{
    public Guid Id { get; set; }

    public int Version { get; set; }

    public long SequenceNumber { get; set; }
}