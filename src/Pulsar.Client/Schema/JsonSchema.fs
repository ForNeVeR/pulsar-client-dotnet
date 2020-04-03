namespace Pulsar.Client.Schema

open System.Text
open System.Text.Json
open Pulsar.Client.Api
open SharpPulsar.Impl.Schema

type JsonSchema<'T>() =
    let options = JsonSerializerOptions(IgnoreNullValues = true)
    
    interface ISchema<'T> with
        member this.Name = ""
        member this.Type = SchemaType.JSON
        member this.SchemaData =
            let s = typeof<'T>.GetSchema()            
            s |> Encoding.UTF8.GetBytes
        member this.Encode value =
            JsonSerializer.SerializeToUtf8Bytes(value, options)

