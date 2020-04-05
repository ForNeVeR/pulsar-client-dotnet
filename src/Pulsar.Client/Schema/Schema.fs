namespace Pulsar.Client.Api

open System.Runtime.InteropServices
open Pulsar.Client.Schema
open System.Text

[<AbstractClass>]
type Schema =
    static member BYTES() =
        BytesSchema() :> ISchema<byte[]>
    static member BOOL() =
        BooleanSchema() :> ISchema<bool>
    static member STRING ([<Optional; DefaultParameterValue(null:Encoding)>]charset: Encoding) =
        let charset = if isNull charset then Encoding.UTF8 else charset
        StringSchema(charset) :> ISchema<string>        
    static member JSON<'T> () =
        JsonSchema<'T>() :> ISchema<'T>
    static member PROTOBUF<'T> () =
        ProtobufSchema<'T>() :> ISchema<'T>
    static member AVRO<'T> () =
        AvroSchema<'T>() :> ISchema<'T>
        
//    static member KEY_VALUE<'K,'V> kvType =
//        Schema<KeyValuePair<'K, 'V>>.KeyValue kvType