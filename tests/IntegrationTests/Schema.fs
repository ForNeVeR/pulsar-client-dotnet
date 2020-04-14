module Pulsar.Client.IntegrationTests.Schema

open Expecto
open Expecto.Flip
open Serilog
open Pulsar.Client.IntegrationTests.Common
open System
open System.Collections.Generic
open System.Text
open System.Text.Json
open Pulsar.Client.Api
open FSharp.UMX

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
                client.NewConsumer(Schema.STRING())
                    .Topic(topicName)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() |> Async.AwaitTask

            let sentText = "Hello schema"
            let! _ = producer.SendAsync(sentText) |> Async.AwaitTask

            let! msg = consumer.ReceiveAsync() |> Async.AwaitTask

            Expect.equal "" sentText msg.Value

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
                client.NewConsumer(Schema.JSON<SimpleRecord>())
                    .Topic(topicName)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() |> Async.AwaitTask

            let input = { Name = "abc"; Age = 20  }
            let! _ = producer.SendAsync(input) |> Async.AwaitTask

            let! msg = consumer.ReceiveAsync() |> Async.AwaitTask

            Expect.equal "" input msg.Value

            Log.Debug("Finished Json schema works fine")
        }
        
        testAsync "KeyValue schema separated works fine" {

            Log.Debug("Start KeyValue schema separated works fine")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")

            let! producer =
                client.NewProducer(Schema.KEY_VALUE(Schema.BOOL(), Schema.STRING(), KeyValueEncodingType.SEPARATED))
                    .Topic(topicName)
                    .EnableBatching(false)
                    .CreateAsync() |> Async.AwaitTask

            let! consumer =
                client.NewConsumer(Schema.KEY_VALUE(Schema.BOOL(), Schema.STRING(), KeyValueEncodingType.SEPARATED))
                    .Topic(topicName)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() |> Async.AwaitTask

            let! _ = producer.SendAsync(KeyValuePair(true, "one")) |> Async.AwaitTask

            let! msg = consumer.ReceiveAsync() |> Async.AwaitTask
            
            Expect.isTrue "" msg.IsKeyBase64Encoded
            Expect.equal "" "one" msg.Value.Value
            Expect.equal "" true msg.Value.Key

            Log.Debug("Finished KeyValue schema separated works fine")
        }
        
        testAsync "KeyValue schema inline works fine" {

            Log.Debug("Start KeyValue schema inline works fine")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")

            let! producer =
                client.NewProducer(Schema.KEY_VALUE(Schema.BOOL(), Schema.STRING(), KeyValueEncodingType.INLINE))
                    .Topic(topicName)
                    .EnableBatching(false)
                    .CreateAsync() |> Async.AwaitTask

            let! consumer =
                client.NewConsumer(Schema.KEY_VALUE(Schema.BOOL(), Schema.STRING(), KeyValueEncodingType.INLINE))
                    .Topic(topicName)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() |> Async.AwaitTask

            let! _ = producer.SendAsync(KeyValuePair(true, "one")) |> Async.AwaitTask

            let! msg = consumer.ReceiveAsync() |> Async.AwaitTask
            
            Expect.isFalse "" msg.IsKeyBase64Encoded
            Expect.equal "" "one" msg.Value.Value
            Expect.equal "" true msg.Value.Key

            Log.Debug("Finished KeyValue schema inline works fine")
        }
    ]
    