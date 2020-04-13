namespace Pulsar.Client.Schema

open ProtoBuf
open System.IO
open Pulsar.Client.Common
open System.Text
open System.Text.Json
open Pulsar.Client.Api
open SharpPulsar.Impl.Schema

type ProtobufSchema<'T>() =
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
        member this.Decode bytes =
            use stream = new MemoryStream(bytes)
            Serializer.Deserialize(stream)

