namespace Pulsar.Client.Schema

open System.Collections.Generic
open Pulsar.Client.Api

type KeyValueSchema<'K,'V>(kvType: KeyValueEncodingType) =
    
    interface ISchema<KeyValuePair<'K,'V>> with
        member this.Name = "KeyValue"
        member this.Type = SchemaType.KEY_VALUE
        member this.SchemaData = [||]
        member this.Encode value =
            [||]

