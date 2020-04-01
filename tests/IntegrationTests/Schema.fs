module Pulsar.Client.IntegrationTests.Schema

open System.Threading.Tasks
open Expecto
open Expecto.Flip

open Serilog
open Pulsar.Client.IntegrationTests.Common
open System
open System.Text
open System.Text
open FSharp.Control.Tasks.V2.ContextInsensitive
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
                    .EnableBatching(false)
                    .CreateAsync() |> Async.AwaitTask

            let! consumer =
                client.NewConsumer()
                    .Topic(topicName)
                    .SubscriptionName("test-subscription")
                    .ReceiverQueueSize(10)
                    .SubscribeAsync() |> Async.AwaitTask

            let sentText = "Hello schema"
            let! _ = producer.SendAsync(sentText) |> Async.AwaitTask

            let! msg = consumer.ReceiveAsync() |> Async.AwaitTask
            let receivedText = msg.Data |> Encoding.UTF8.GetString

            Expect.equal "" sentText receivedText

            Log.Debug("Finished String schema works fine")
        }
        
        ftestAsync "Json schema works fine" {

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
                    .ReceiverQueueSize(10)
                    .SubscribeAsync() |> Async.AwaitTask

            let sentText = "Hello schema"
            let! _ = producer.SendAsync({ Name = "abc"; Age = 20  }) |> Async.AwaitTask

            let! msg = consumer.ReceiveAsync() |> Async.AwaitTask
            let receivedText = msg.Data |> Encoding.UTF8.GetString

            Expect.equal "" sentText receivedText

            Log.Debug("Finished String schema works fine")
        }
    ]
    