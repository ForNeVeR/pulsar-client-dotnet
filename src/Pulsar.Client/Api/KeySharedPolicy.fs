﻿namespace Pulsar.Client.Api

open System

type Range (starts: int, ends: int) =
    do
        if ends < starts then
            raise <| ArgumentException("Range end must >= range start.")          
    member this.Start = starts    
    member this.End = ends
            
    member this.Intersect (range: Range) =
        let starts = if range.Start > this.Start then range.Start else this.Start
        let ends = if range.End < this.End then range.End else this.End
        if ends >= starts then
            Some <| Range(starts, ends)
        else
            None
            
    override this.ToString() =
        sprintf "[%i, %i]" starts ends    

[<AbstractClass>]
type KeySharedPolicy internal () =
    abstract member Validate: unit -> unit    
    static member DEFAULT_HASH_RANGE_SIZE = 2 <<< 15
    
    static member KeySharedPolicySticky(ranges: Range[]) = KeySharedPolicySticky(ranges)
    static member KeySharedPolicyAutoSplit() = KeySharedPolicyAutoSplit()
    
and KeySharedPolicySticky internal (ranges: Range[]) =
    inherit KeySharedPolicy()
    override this.Validate() =
        if (isNull ranges) || (ranges.Length = 0) then
            raise <| ArgumentException("Ranges for KeyShared policy must not be empty.")
        for range1 in ranges do
            if range1.Start < 0 || range1.End > KeySharedPolicy.DEFAULT_HASH_RANGE_SIZE then
                raise <| ArgumentException("Ranges must be [0, 65535] but provided range is " + range1.ToString())
            for range2 in ranges do
                if range1 <> range2 && range1.Intersect(range2).IsSome then
                    raise <| ArgumentException("Ranges for KeyShared policy with overlap between " +  range1.ToString() +
                                               " and " + range2.ToString())
    member this.Ranges = ranges
    
and KeySharedPolicyAutoSplit internal () =
    inherit KeySharedPolicy()
    
    override this.Validate() = ()