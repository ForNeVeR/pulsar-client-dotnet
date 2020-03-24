namespace Pulsar.Client.Api

type Schema<'T> =
    | Bool
    | String
    | Json

module Schema =
    let BOOL = Schema<bool>.Bool   
    let STRING = Schema<string>.String    
    let JSON<'T> = Schema<'T>.Json
    
    