namespace Pulsar.Client.Schema

open Pulsar.Client.Api

type StringSchema internal (charset: System.Text.Encoding) =
    interface ISchema<string> with
        member this.Name = "String"
        member this.Type = SchemaType.STRING
        member this.SchemaData= [||]
        member this.Encode value =
            charset.GetBytes(value)            
        member this.Decode bytes =
            charset.GetString(bytes)

