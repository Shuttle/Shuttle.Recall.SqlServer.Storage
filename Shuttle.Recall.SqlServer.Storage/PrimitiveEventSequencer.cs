namespace Shuttle.Recall.SqlServer.Storage;

public class PrimitiveEventSequencer : IPrimitiveEventSequencer
{
    public async ValueTask<bool> SequenceAsync()
    {
        return false;
    }
}