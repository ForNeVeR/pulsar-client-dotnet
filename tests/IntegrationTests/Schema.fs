module Pulsar.Client.IntegrationTests.Schema

open Expecto
open Expecto.Flip

open Serilog
open Pulsar.Client.IntegrationTests.Common
open System
open System.Text
open System.Text.Json
open Pulsar.Client.Api

[<CLIMutable>]
type SimpleRecord =
    {
        Name: string
        Age: int
    }

[<Tests>]
let tests =

    testList "schema" [

        testAsync "String schema works fine" {

            Log.Debug("Start String schema works fine")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")

            let! producer =
                client.NewProducer(Schema.STRING())
                    .Topic(topicName)
                    .CreateAsync() |> Async.AwaitTask

            let! consumer =
                client.NewConsumer()
                    .Topic(topicName)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() |> Async.AwaitTask

            let sentText = "Hello schema"
            let! _ = producer.SendAsync(sentText) |> Async.AwaitTask

            let! msg = consumer.ReceiveAsync() |> Async.AwaitTask
            let receivedText = msg.Data |> Encoding.UTF8.GetString

            Expect.equal "" sentText receivedText

            Log.Debug("Finished String schema works fine")
        }
        
        testAsync "Json schema works fine" {

            Log.Debug("Start Json schema works fine")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")

            let! producer =
                client.NewProducer(Schema.JSON<SimpleRecord>())
                    .Topic(topicName)
                    .EnableBatching(false)
                    .CreateAsync() |> Async.AwaitTask

            let! consumer =
                client.NewConsumer()
                    .Topic(topicName)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() |> Async.AwaitTask

            let input = { Name = "abc"; Age = 20  }
            let! _ = producer.SendAsync(input) |> Async.AwaitTask

            let! msg = consumer.ReceiveAsync() |> Async.AwaitTask
            let output = JsonSerializer.Deserialize<SimpleRecord>(ReadOnlySpan msg.Data)

            Expect.equal "" input output

            Log.Debug("Finished Json schema works fine")
        }
    ]
    