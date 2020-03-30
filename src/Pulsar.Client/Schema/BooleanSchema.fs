namespace Pulsar.Client.Schema

open Pulsar.Client.Api

type BooleanSchema() =
    interface ISchema<bool> with
        member this.Name = "Boolean"
        member this.Type = SchemaType.BOOLEAN
        member this.SchemaData= [||]
        member this.Encode value = [| if value then 1uy else 0uy |]

