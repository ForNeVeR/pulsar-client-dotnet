namespace Pulsar.Client.Api

open System
open System.Threading.Tasks
open Pulsar.Client.Common

type ReaderBuilder private (createReaderAsync, config: ReaderConfiguration) =

    let verify(config : ReaderConfiguration) =
        let checkValue check config =
            check config |> ignore
            config

        config
        |> checkValue
            (fun c ->
                c.Topic
                |> invalidArgIfDefault "Topic name must be set on the reader builder.")
        |> checkValue
            (fun c ->
                if c.StartMessageId.IsSome && c.StartMessageFromRollbackDuration > TimeSpan.Zero
                    || c.StartMessageId.IsNone && c.StartMessageFromRollbackDuration = TimeSpan.Zero then                
                    failwith "Start message id or start message from roll back must be specified but they cannot be specified at the same time"
                elif c.StartMessageFromRollbackDuration > TimeSpan.Zero then
                    { config with
                        StartMessageId = Some MessageId.Earliest }
                else
                    c)

    internal new(createReaderAsync) = ReaderBuilder(createReaderAsync, ReaderConfiguration.Default)
    
    member private this.With(newConfig) =
        ReaderBuilder(createReaderAsync, newConfig)

    member this.Topic topic =        
        { config with
            Topic = topic
                |> invalidArgIfBlankString "Topic must not be blank."
                |> fun t -> TopicName(t.Trim()) }
        |> this.With

    member this.StartMessageId messageId =        
        { config with
            StartMessageId = messageId
                |> invalidArgIfDefault "MessageId can't be null"
                |> Some }
        |> this.With

    member this.StartMessageIdInclusive (startMessageIdInclusive: bool) =        
        { config with
            ResetIncludeHead = startMessageIdInclusive }
        |> this.With

    member this.ReadCompacted readCompacted =
        { config with
            ReadCompacted = readCompacted }
        |> this.With

    member this.SubscriptionRolePrefix subscriptionRolePrefix =    
        { config with
            SubscriptionRolePrefix = subscriptionRolePrefix }
        |> this.With

    member this.ReaderName readerName =
        { config with
            ReaderName = readerName |> invalidArgIfBlankString "ReaderName must not be blank." }
        |> this.With

    member this.ReceiverQueueSize receiverQueueSize =
        { config with
            ReceiverQueueSize = receiverQueueSize |> invalidArgIfNotGreaterThanZero "ReceiverQueueSize should be greater than 0."  }
        |> this.With
                
    member this.StartMessageFromRollbackDuration rollbackDuration =
        { config with
            StartMessageFromRollbackDuration = rollbackDuration }
        |> this.With

    member this.CreateAsync(): Task<Reader> =
        config
        |> verify
        |> createReaderAsync

