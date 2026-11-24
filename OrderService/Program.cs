using System.Text.Json;
using Confluent.Kafka;

var builder = WebApplication.CreateBuilder(args);

// One producer for the whole app. It is thread-safe and expensive to create.

builder.Services.AddSingleton<IProducer<string, string>>(
    new ProducerBuilder<string, string>(
        new ProducerConfig { BootstrapServers = "localhost:9092" }).Build());

var app = builder.Build();

app.MapPost("/orders", async (CreateOrder request, IProducer<string, string> producer) =>
{
    var order = new OrderCreated(Guid.NewGuid(), request.Product, request.Amount, DateTime.UtcNow);

    await producer.ProduceAsync("orders", new Message<string, string>
    {
        Key = order.OrderId.ToString(),
        Value = JsonSerializer.Serialize(order)
    });

    return Results.Accepted($"/orders/{order.OrderId}", order);
});

app.MapGet("/", () => "Hello World!");

app.Run();

record CreateOrder(string Product, decimal Amount);
record OrderCreated(Guid OrderId, string Product, decimal Amount, DateTime CreatedAt);
