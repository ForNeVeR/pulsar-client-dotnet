namespace Pulsar.Client.Schema

open System
open System.Collections.Generic
open System.IO
open Pulsar.Client.Api
open Pulsar.Client.Common

type KeyValueSchema<'K,'V>(keySchema: ISchema<'K>, valueSchema: ISchema<'V>, kvType: KeyValueEncodingType) =
    
    let getKeyValueBytes (keyBytes: byte[]) (valueBytes: byte[]) =
        let result = Array.create (4 + keyBytes.Length + 4 + valueBytes.Length) 0uy
        use stream = new MemoryStream(result)
        use binaryWriter = new BinaryWriter(stream)
        binaryWriter.Write(int32ToBigEndian keyBytes.Length)
        binaryWriter.Write(keyBytes)
        binaryWriter.Write(int32ToBigEndian valueBytes.Length)
        binaryWriter.Write(valueBytes)
        result
        
    member this.KeyValueEncodingType = kvType
    
    member this.KeySchema = keySchema
    
    member this.ValueSchema = valueSchema
    
    interface ISchema<KeyValuePair<'K,'V>> with
        member this.Name = "KeyValue"
        member this.Type = SchemaType.KEY_VALUE
        member this.SchemaData =
            let keyBytes = keySchema.SchemaData
            let valueBytes = valueSchema.SchemaData
            getKeyValueBytes keyBytes valueBytes
        member this.Encode (KeyValue(key, value)) =
            match kvType with
            | KeyValueEncodingType.INLINE ->
                let keyBytes = keySchema.Encode(key)
                let valueBytes = valueSchema.Encode(value)
                getKeyValueBytes keyBytes valueBytes
            | KeyValueEncodingType.SEPARATED ->
                valueSchema.Encode(value)
            | _ ->
                failwith "Unsupported KeyValueEncodingType"

type internal IKeyValueProcessor =
    abstract member GetKeyValue: obj -> struct(string * byte[])
    abstract member EncodingType: KeyValueEncodingType

type internal KeyValueProcessor<'K,'V>(schema: KeyValueSchema<'K,'V>) =
    
    interface IKeyValueProcessor with
        member this.GetKeyValue value =
            let (KeyValue(k, v)) = value :?> KeyValuePair<'K,'V>
            let strKey = schema.KeySchema.Encode(k) |> Convert.ToBase64String
            let content = schema.ValueSchema.Encode(v)
            struct(strKey, content)
                
        member this.EncodingType =
            schema.KeyValueEncodingType
                
type internal KeyValueProcessor =
    static member GetInstance (schema: ISchema<'T>) =
        if schema.Type = SchemaType.KEY_VALUE then
            let kvType = typeof<'T>
            let kvpTypeTemplate = typedefof<KeyValueProcessor<_,_>>
            let kvpType = kvpTypeTemplate.MakeGenericType(kvType.GetGenericArguments())
            let obj = Activator.CreateInstance(kvpType, schema)
            let kvp = (obj :?> IKeyValueProcessor)
            if kvp.EncodingType = KeyValueEncodingType.SEPARATED then
                ValueSome kvp
            else
                ValueNone
        else
            ValueNone
