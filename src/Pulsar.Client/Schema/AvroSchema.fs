namespace Pulsar.Client.Schema

open Avro
open Avro.IO
open Avro.Reflect
open System.IO
open ProtoBuf
open System.Text
open Pulsar.Client.Api
open SharpPulsar.Impl.Schema
open Pulsar.Client.Common

type AvroSchema<'T>() =
    let stringSchema = typeof<'T>.GetSchema()
    let avroSchema = Schema.Parse(stringSchema)    
    let avroWriter = ReflectWriter<'T>(avroSchema)
    let avroReader = ReflectReader<'T>(avroSchema, avroSchema)
    
    interface ISchema<'T> with
        member this.Name = "" 
        member this.Type = SchemaType.AVRO
        member this.SchemaData =      
            stringSchema |> Encoding.UTF8.GetBytes
        member this.Encode value =
            use stream = MemoryStreamManager.GetStream()
            avroWriter.Write(value, BinaryEncoder(stream))
            stream.ToArray()
        member this.Decode bytes =
            use stream = new MemoryStream(bytes)
            avroReader.Read(Unchecked.defaultof<'T>, BinaryDecoder(stream))