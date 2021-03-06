﻿namespace Pulsar.Client.Api

open Pulsar.Client.Common
open Pulsar.Client.Internal
open System

type ConsumerBuilder private (client: PulsarClient, config: ConsumerConfiguration, consumerInterceptors: ConsumerInterceptors) =

    [<Literal>]
    let MIN_ACK_TIMEOUT_MILLIS = 1000

    [<Literal>]
    let DEFAULT_ACK_TIMEOUT_MILLIS_FOR_DEAD_LETTER = 30000.0

    let verify(config : ConsumerConfiguration) =
        let checkValue check config =
            check config |> ignore
            config

        config
        |> checkValue
            (fun c ->
                c.Topic
                |> invalidArgIfDefault "Topic name must be set on the consumer builder.")
        |> checkValue
            (fun c ->
                c.SubscriptionName
                |> invalidArgIfBlankString "Subscription name must be set on the consumer builder.")
        |> checkValue
            (fun c ->
                invalidArgIfTrue (
                    c.ReadCompacted && (not c.Topic.IsPersistent ||
                        (c.SubscriptionType <> SubscriptionType.Exclusive && c.SubscriptionType <> SubscriptionType.Failover ))
                ) "Read compacted can only be used with exclusive of failover persistent subscriptions")
        |> checkValue
            (fun c ->
                invalidArgIfTrue (
                    c.KeySharedPolicy.IsSome && c.SubscriptionType <> SubscriptionType.KeyShared
                ) "KeySharedPolicy must be set with KeyShared subscription")

    new(client: PulsarClient) = ConsumerBuilder(client, ConsumerConfiguration.Default, ConsumerInterceptors.Empty)

    member private this.With(newConfig: ConsumerConfiguration) =
        ConsumerBuilder(client, newConfig, consumerInterceptors)

    member private this.With(newInterceptors: ConsumerInterceptors) =
        ConsumerBuilder(client, config, newInterceptors)
    
    member this.Topic topic =
        { config with
            Topic = topic
                |> invalidArgIfBlankString "Topic must not be blank."
                |> fun t -> TopicName(t.Trim()) }
        |> this.With

    member this.ConsumerName name =
        { config with
            ConsumerName = name |> invalidArgIfBlankString "Consumer name must not be blank." }
        |> this.With

    member this.SubscriptionName subscriptionName =
        { config with
            SubscriptionName = subscriptionName |> invalidArgIfBlankString "Subscription name must not be blank." }
        |> this.With        

    member this.SubscriptionType subscriptionType =
        { config with
            SubscriptionType = subscriptionType  }
        |> this.With

    member this.SubscriptionMode subscriptionMode =
        { config with
            SubscriptionMode = subscriptionMode  }
        |> this.With
    
    member this.ReceiverQueueSize receiverQueueSize =
        { config with
            ReceiverQueueSize = receiverQueueSize |> invalidArgIfNotGreaterThanZero "ReceiverQueueSize should be greater than 0."  }
        |> this.With

    member this.SubscriptionInitialPosition subscriptionInitialPosition =
        { config with
            SubscriptionInitialPosition = subscriptionInitialPosition  }
        |> this.With

    member this.AckTimeout ackTimeout =
        { config with
            AckTimeout = ackTimeout |> invalidArgIf (fun arg ->
               arg <> TimeSpan.Zero && arg < TimeSpan.FromMilliseconds(float MIN_ACK_TIMEOUT_MILLIS)) (sprintf "Ack timeout should be greater than %i ms" MIN_ACK_TIMEOUT_MILLIS)  }
        |> this.With

    member this.AckTimeoutTickTime ackTimeoutTickTime =
        { config with
            AckTimeoutTickTime = ackTimeoutTickTime  }
        |> this.With

    member this.AcknowledgementsGroupTime ackGroupTime =
        { config with
            AcknowledgementsGroupTime = ackGroupTime  }
        |> this.With

    member this.ReadCompacted readCompacted =
        { config with
            ReadCompacted = readCompacted  }
        |> this.With

    member this.NegativeAckRedeliveryDelay negativeAckRedeliveryDelay =
        { config with
            NegativeAckRedeliveryDelay = negativeAckRedeliveryDelay  }
        |> this.With

    member this.DeadLettersPolicy (deadLettersPolicy: DeadLettersPolicy) =

        let ackTimeoutTickTime =
            if config.AckTimeoutTickTime = TimeSpan.Zero
            then TimeSpan.FromMilliseconds(DEFAULT_ACK_TIMEOUT_MILLIS_FOR_DEAD_LETTER)
            else config.AckTimeoutTickTime

        let getTopicName() = config.Topic.ToString()
        let getSubscriptionName() = config.SubscriptionName
        let createProducer deadLetterTopic =
            ProducerBuilder(client)
                .Topic(deadLetterTopic)
                .EnableBatching(false) // dead letters are sent one by one anyway
                .CreateAsync()
        let deadLettersProcessor =
            DeadLettersProcessor(deadLettersPolicy, getTopicName, getSubscriptionName, createProducer) :> IDeadLettersProcessor

        { config with
            AckTimeoutTickTime = ackTimeoutTickTime
            DeadLettersProcessor = deadLettersProcessor }
        |> this.With

    member this.StartMessageIdInclusive () =
        { config with
            ResetIncludeHead = true }
        |> this.With

    member this.BatchReceivePolicy (batchReceivePolicy: BatchReceivePolicy) =
        { config with
            BatchReceivePolicy = batchReceivePolicy
                |> invalidArgIfDefault "BatchReceivePolicy can't be null"
                |> fun policy -> policy.Verify(); policy }
        |> this.With

    member this.KeySharedPolicy (keySharedPolicy: KeySharedPolicy) =
        { config with
            KeySharedPolicy = keySharedPolicy
                |> invalidArgIfDefault "KeySharedPolicy can't be null"
                |> fun policy -> keySharedPolicy.Validate(); policy
                |> Some }
        |> this.With

    member this.Intercept ([<ParamArray>] interceptors: IConsumerInterceptor array) =
        if interceptors.Length = 0 then this
        else
            ConsumerInterceptors(Array.append consumerInterceptors.Interceptors interceptors)
            |> this.With
    
    member this.SubscribeAsync() =
        config
        |> verify
        |> client.SubscribeAsync consumerInterceptors
