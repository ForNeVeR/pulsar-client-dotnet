module Pulsar.Client.IntegrationTests.Common

open System
open Expecto
open Expecto.Flip
open Pulsar.Client.Api
open FSharp.Control.Tasks.V2.ContextInsensitive
open System.Text
open System.Threading.Tasks
open Pulsar.Client.Common
open Serilog
open Microsoft.Extensions.Logging
open Microsoft.Extensions.DependencyInjection
open Serilog.Sinks.SystemConsole.Themes
open System.Collections.Generic
open Pulsar.Client.IntegrationTests
open FSharp.UMX


[<Literal>]
let pulsarAddress = "pulsar://my-pulsar-cluster:31002"

let configureLogging() =
    Log.Logger <-
        LoggerConfiguration()
            .MinimumLevel.Debug()
            .WriteTo.Console(theme = AnsiConsoleTheme.Code, outputTemplate="[{Timestamp:HH:mm:ss} {Level:u3} {ThreadId}] {Message:lj}{NewLine}{Exception}")
            .Enrich.FromLogContext()
            .Enrich.WithThreadId()
            .CreateLogger()
    let serviceCollection = new ServiceCollection()
    let sp =
        serviceCollection
            .AddLogging(fun configure -> configure.AddSerilog(dispose = true) |> ignore)
            .BuildServiceProvider()
    let logger = sp.GetService<ILogger<PulsarClient>>()
    PulsarClient.Logger <- logger



configureLogging()

let commonClient =
    PulsarClientBuilder()
        .ServiceUrl(pulsarAddress)
        .Build()

let getClient() = commonClient
let getNewClient() =
    PulsarClientBuilder()
        .ServiceUrl(pulsarAddress)
        .Build()

let produceMessages (producer: Producer) number producerName =
    task {
        for i in [1..number] do
            let! _ = producer.SendAndWaitAsync(Encoding.UTF8.GetBytes(sprintf "Message #%i Sent from %s on %s" i producerName (DateTime.Now.ToLongTimeString()) ))
            ()
    }

let fastProduceMessages (producer: Producer) number producerName =
    task {
        for i in [1..number] do
            let! _ = producer.SendAsync(Encoding.UTF8.GetBytes(sprintf "Message #%i Sent from %s on %s" i producerName (DateTime.Now.ToLongTimeString()) ))
            ()
    }

let createSendAndWaitTasks (producer: Producer) number producerName =
    let createTask taskNumber =
        let message = sprintf "Message #%i Sent from %s on %s" taskNumber producerName (DateTime.Now.ToLongTimeString())
        let messageBytes = Encoding.UTF8.GetBytes(message)
        let task = Task.Run(fun() -> producer.SendAndWaitAsync(messageBytes) |> ignore)
        (task, message)

    [|1..number|] |> Array.map createTask

let getMessageNumber (msg: string) =
    let ind1 = msg.IndexOf("#")
    let ind2 = msg.IndexOf("Sent")
    let subString = msg.Substring(ind1+1, ind2 - ind1 - 2)
    int subString

let consumeMessages (consumer: Consumer) number consumerName =
    task {
        for i in [1..number] do
            let! message = consumer.ReceiveAsync()
            let received = Encoding.UTF8.GetString(message.Payload)
            do! consumer.AcknowledgeAsync(message.MessageId)
            Log.Debug("{0} received {1}", consumerName, received)
            let expected = "Message #" + string i
            if received.StartsWith(expected) |> not then
                failwith <| sprintf "Incorrect message expected %s received %s consumer %s" expected received consumerName
    }

let consumeAndVerifyMessages (consumer: Consumer) consumerName (expectedMessages : string[]) =
    task {
        for i in [1..expectedMessages.Length] do
            let! message = consumer.ReceiveAsync()
            let received = Encoding.UTF8.GetString(message.Payload)
            do! consumer.AcknowledgeAsync(message.MessageId)
            Log.Debug("{0} received {1}", consumerName, received)
            if expectedMessages |> Array.contains received |> not then
                failwith <| sprintf "Received unexpected message '%s' consumer %s" received consumerName
    }