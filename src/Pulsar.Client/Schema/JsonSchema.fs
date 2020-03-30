namespace Pulsar.Client.Schema

open System.Text.Json
open Pulsar.Client.Api

type JsonSchema<'T>() =
    let options = JsonSerializerOptions(IgnoreNullValues = true)
    
    interface ISchema<'T> with
        member this.Name = ""
        member this.Type = SchemaType.JSON
        member this.SchemaData =
            [||]
        member this.Encode value =
            JsonSerializer.SerializeToUtf8Bytes(value, options)

