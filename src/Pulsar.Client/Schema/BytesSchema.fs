namespace Pulsar.Client.Schema

open Pulsar.Client.Api
open pulsar.proto

type BytesSchema() =
    interface ISchema<byte[]> with
        member this.Name = "Bytes"
        member this.Type = SchemaType.BYTES
        member this.SchemaData= [||]
        member this.Encode value = value

