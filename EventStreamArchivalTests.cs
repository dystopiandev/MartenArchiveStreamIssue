using Marten;
using Marten.Events;
using Marten.Storage;
using Testcontainers.PostgreSql;
using Xunit;

namespace MartenIssue;

public class EventStreamArchivalTests : IAsyncLifetime
{
    private string _connectionString = null!;
    private readonly PostgreSqlContainer _postgresContainer = new PostgreSqlBuilder()
        .WithImage("postgres:17.4-alpine3.21")
        .WithDatabase("test_db")
        .WithUsername("test_user")
        .WithPassword("test_password")
        .Build();

    public async Task InitializeAsync()
    {
        await _postgresContainer.StartAsync();
        _connectionString = _postgresContainer.GetConnectionString();
    }

    public async Task DisposeAsync()
    {
        await _postgresContainer.DisposeAsync();
    }

    [Fact]
    public async Task ArchiveStream_ShouldMarkStreamAsArchived()
    {
        // ARRANGE
        const string streamKey = "test-stream";
        const string tenantId = "test-tenant";

        DocumentStore store = DocumentStore.For(options =>
        {
            options.Connection(_connectionString);
            options.Events.StreamIdentity = StreamIdentity.AsString;
            options.Events.TenancyStyle = TenancyStyle.Conjoined;
            options.Events.UseArchivedStreamPartitioning = true;
        });
        await using IDocumentSession session = store.LightweightSession();

        // ACT
        session.ForTenant(tenantId).Events.Append(streamKey, new TestEvent(1), new TestEvent(2));
        await session.SaveChangesAsync();

        session.ForTenant(tenantId).Events.ArchiveStream(streamKey);
        await session.SaveChangesAsync();

        StreamState? state = await session.ForTenant(tenantId).Events.FetchStreamStateAsync(streamKey);

        // ASSERT
        Assert.NotNull(state);
        Assert.Equal(streamKey, state.Key);
        Assert.True(state.IsArchived);
    }

    internal record TestEvent(int Id);
}
