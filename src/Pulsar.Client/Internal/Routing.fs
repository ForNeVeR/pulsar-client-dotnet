namespace Pulsar.Client.Internal

open System
open Pulsar.Client.Common
open System.Threading
open Pulsar.Client.Api
open FSharp.UMX

type internal SinglePartitionMessageRouterImpl (partitionIndex: int, hashFun: string -> int) =
    interface IMessageRouter with
        member this.ChoosePartition (messageKey, numPartitions) =
            let stringKey = messageKey.GetKey()
            if String.IsNullOrEmpty(stringKey) then
                partitionIndex
            else
                // If the message has a key, it supersedes the single partition routing policy
                signSafeMod (hashFun stringKey) numPartitions

type internal RoundRobinPartitionMessageRouterImpl (startPartitionIndex: int, isBatchingEnabled: bool, partitionSwitchMs: int, hashFun: string -> int) =
    let mutable partitionIndex = startPartitionIndex
    let partitionSwitchMs = Math.Max(1, partitionSwitchMs)

    interface IMessageRouter with
        member this.ChoosePartition (messageKey, numPartitions) =
            let stringKey = messageKey.GetKey()
            if String.IsNullOrEmpty(%stringKey) then
                if isBatchingEnabled
                then
                    let currentMs = DateTime.Now.Millisecond
                    signSafeMod (currentMs / partitionSwitchMs + startPartitionIndex) numPartitions
                else
                    signSafeMod (Interlocked.Increment(&partitionIndex)) numPartitions
            else
                // If the message has a key, it supersedes the single partition routing policy
                signSafeMod (hashFun %stringKey) numPartitions
