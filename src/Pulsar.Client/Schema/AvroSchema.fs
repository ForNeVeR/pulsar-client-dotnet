namespace Pulsar.Client.Schema

open ProtoBuf
open System.Text
open Pulsar.Client.Api
open SharpPulsar.Impl.Schema
open Pulsar.Client.Common

type AvroSchema<'T>() =
    let stringSchema = typeof<'T>.GetSchema()
    
    interface ISchema<'T> with
        member this.Name = "" 
        member this.Type = SchemaType.PROTOBUF
        member this.SchemaData =
            stringSchema |> Encoding.UTF8.GetBytes
        member this.Encode value =
            use stream = MemoryStreamManager.GetStream()
            Serializer.Serialize(stream, value)
            stream.ToArray()