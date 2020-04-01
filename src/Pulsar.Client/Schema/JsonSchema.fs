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
            let z = """{"type":"record","namespace":"Pulsar.Client.IntegrationTests","name":"Pulsar.Client.IntegrationTests.SimpleRecord","fields":[{"name":"Name","type":["string","null"]},{"name":"Age","type":["string","null"]}]}"""
            z |> Encoding.UTF8.GetBytes
        member this.Encode value =
            JsonSerializer.SerializeToUtf8Bytes(value, options)

