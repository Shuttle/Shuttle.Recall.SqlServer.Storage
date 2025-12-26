using Microsoft.EntityFrameworkCore;

namespace Shuttle.Recall.SqlServer.Storage;

public class SqlServerStorageDbContext(DbContextOptions<SqlServerStorageDbContext> options) : DbContext(options);