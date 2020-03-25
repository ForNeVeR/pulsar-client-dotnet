namespace Pulsar.Client.Api

type Schema<'T> =
    | Bytes
    | Bool
    | String
    | Json

module Schema =
    let BYTES = Schema<byte[]>.Bytes
    let BOOL = Schema<bool>.Bool
    let STRING = Schema<string>.String    
    let JSON<'T> = Schema<'T>.Json
    
    