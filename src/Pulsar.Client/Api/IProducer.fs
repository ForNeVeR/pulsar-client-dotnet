namespace Pulsar.Client.Api

open System
open System.Threading.Tasks
open Pulsar.Client.Common

type IProducer<'T> =
    inherit IAsyncDisposable

    /// Send message and await confirmation from broker
    abstract member SendAsync: message:'T -> Task<MessageId>
    /// Send message with keys and props
    abstract member SendAsync: messageBuilder:MessageBuilder<'T> -> Task<MessageId>
    /// Complete as soon as message gets in client's internal message queue, don't wait for any confirmations
    abstract member SendAndForgetAsync: message:'T -> Task<unit>
    /// Complete as soon as message gets in client's internal message queue, don't wait for any confirmations
    abstract member SendAndForgetAsync: messageBuilder:MessageBuilder<'T> -> Task<unit>
    /// Internal client producer id
    abstract member ProducerId: ProducerId
    /// Get the topic which producer is publishing to
    abstract member Topic: string