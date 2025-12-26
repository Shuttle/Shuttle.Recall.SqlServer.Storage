using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore;
using Shuttle.Core.Contract;

namespace Shuttle.Recall.SqlServer.Storage.Models;

[PrimaryKey(nameof(Id), nameof(System.Version))]
public class PrimitiveEvent
{
    public Guid Id { get; set; }

    public int Version { get; set; }

    public byte[] EventEnvelope { get; set; } = null!;

    public Guid EventId { get; set; }

    public Guid EventTypeId { get; set; }

    public long? SequenceNumber { get; set; }

    public DateTime DateRegistered { get; set; }

    public Guid? CorrelationId { get; set; }

    public EventType EventType { get; set; } = null!;

    public class Specification
    {
        private readonly List<Type> _eventTypes = [];
        private readonly List<Guid> _ids = [];
        public IEnumerable<Type> EventTypes => _eventTypes.AsReadOnly();

        public IEnumerable<Guid> Ids => _ids.AsReadOnly();
        public long SequenceNumberStart { get; private set; }
        public int MaximumRows { get; private set; }

        public Specification AddEventType<T>()
        {
            AddEventType(typeof(T));

            return this;
        }

        public Specification AddEventType(Type type)
        {
            Guard.AgainstNull(type, nameof(type));

            if (!_eventTypes.Contains(type))
            {
                _eventTypes.Add(type);
            }

            return this;
        }

        public Specification AddEventTypes(IEnumerable<Type>? types)
        {
            foreach (var type in types ?? [])
            {
                AddEventType(type);
            }

            return this;
        }

        public Specification AddId(Guid id)
        {
            Guard.AgainstNull(id, nameof(id));

            if (!_ids.Contains(id))
            {
                _ids.Add(id);
            }

            return this;
        }

        public Specification AddIds(IEnumerable<Guid> ids)
        {
            foreach (var type in ids)
            {
                AddId(type);
            }

            return this;
        }

        public Specification WithSequenceNumberStart(long sequenceNumberStart)
        {
            SequenceNumberStart = sequenceNumberStart;

            return this;
        }

        public Specification WithMaximumRows(int maximumRows)
        {
            MaximumRows = maximumRows;

            return this;
        }
    }
}