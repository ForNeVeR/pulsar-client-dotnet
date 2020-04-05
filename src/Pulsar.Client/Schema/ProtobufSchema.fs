namespace Pulsar.Client.Schema

open System.Text
open System.Text.Json
open Pulsar.Client.Api
open SharpPulsar.Impl.Schema

type ProtobufSchema<'T>() =
    let options = JsonSerializerOptions(IgnoreNullValues = true)
    let stringSchema = typeof<'T>.GetSchema()      
    
    interface ISchema<'T> with
        member this.Name = ""
        member this.Type = SchemaType.JSON
        member this.SchemaData =      
            stringSchema |> Encoding.UTF8.GetBytes
        member this.Encode value =
            JsonSerializer.SerializeToUtf8Bytes(value, options)

