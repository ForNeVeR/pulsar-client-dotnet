namespace Pulsar.Client.Schema

open Pulsar.Client.Api
open Pulsar.Client.Common

type BooleanSchema() =
    interface ISchema<bool> with
        member this.Name = "Boolean"
        member this.Type = SchemaType.BOOLEAN
        member this.SchemaData= [||]
        member this.Encode value = [| if value then 1uy else 0uy |]
        member this.Decode bytes =
            if (bytes.Length <> 1) then
                raise <| SchemaSerializationException "Size of data received by BooleanSchema is not 1"
            bytes.[0] <> 0uy

