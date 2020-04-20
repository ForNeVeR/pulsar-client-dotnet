namespace Pulsar.Client.Schema

open System
open System.Text
open System.Text.Json
open Pulsar.Client.Api
open AvroSchemaGenerator

type JsonSchema<'T>() =
    let options = JsonSerializerOptions(IgnoreNullValues = true)
    let stringSchema = typeof<'T>.GetSchema()
    
    interface ISchema<'T> with
        member this.Name = ""
        member this.Type = SchemaType.JSON
        member this.SchemaData =      
            stringSchema |> Encoding.UTF8.GetBytes
        member this.Encode value =
            JsonSerializer.SerializeToUtf8Bytes(value, options)            
        member this.Decode bytes =
            JsonSerializer.Deserialize<'T>(ReadOnlySpan bytes, options)

