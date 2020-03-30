namespace Pulsar.Client.Api

open Pulsar.Client.Schema
open Pulsar.Client.Schema
open System.Text

[<AbstractClass>]
type Schema =
    static member BYTES() =
        BytesSchema() :> ISchema<byte[]>
    static member BOOL() =
        BooleanSchema() :> ISchema<bool>
    static member STRING () =
        StringSchema(Encoding.UTF8) :> ISchema<string>
    static member STRING (charset: Encoding) =
        StringSchema(charset) :> ISchema<string>
        
    static member JSON<'T> () =
        JsonSchema<'T>() :> ISchema<'T>
        
        
//    let JSON<'T> = Schema<'T>.Json
//    let KEY_VALUE<'K,'V> kvType = Schema<KeyValuePair<'K, 'V>>.KeyValue kvType