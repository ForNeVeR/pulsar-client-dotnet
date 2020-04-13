namespace Pulsar.Client.Api

type KeyValueEncodingType =
    | SEPARATED = 0
    | INLINE = 1

type SchemaType =
    | NONE = 0
    | STRING = 1
    | JSON = 2
    | PROTOBUF = 3
    | AVRO = 4
    | BOOLEAN = 5
    | INT8 = 6
    | INT16 = 7
    | INT32 = 8
    | INT64 = 9
    | FLOAT = 10
    | DOUBLE = 11
    | DATE = 12
    | TIME = 13
    | TIMESTAMP = 14
    | KEY_VALUE = 15
    | BYTES = -1
    | AUTO_CONSUME = -3
    | AUTO_PUBLISH = -4    
    
    
type ISchema<'T> =
    abstract member Name: string
    abstract member Type: SchemaType
    abstract member SchemaData: byte[]
    abstract member Encode: 'T -> byte[]
    abstract member Decode: byte[] -> 'T