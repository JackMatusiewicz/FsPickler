namespace MBrace.FsPickler.Json

open System
open System.Collections.Generic
open System.Globalization
open System.IO
open System.Numerics
open System.Text

open Newtonsoft.Json

open MBrace.FsPickler

/// <summary>
///     Json format deserializer
/// </summary>
type internal JsonPickleReader (jsonReader : JsonReader, omitHeader, useCustomSeparator, isTopLevelSequence, leaveOpen) =

    do
        jsonReader.CloseInput <- not leaveOpen
        jsonReader.SupportMultipleContent <- isTopLevelSequence
        jsonReader.DateParseHandling <- DateParseHandling.None

    let isBsonReader = match jsonReader with :? Bson.BsonReader -> true | _ -> false
    let isTextReader = match jsonReader with :? JsonTextReader -> true | _ -> false
    
    // Json.NET 8 introduces bug when SupportMultipleContent is enabled 
    let isJsonDotNet8CustomSequence = 
        isTopLevelSequence && 
        useCustomSeparator &&
        isTextReader &&
        jsonDotNetVersion.Major >= 8

    let mutable depth = 0
    let arrayStack = new Stack<int> ()
    do arrayStack.Push Int32.MinValue

    let cache = System.Collections.Generic.Dictionary<(string * int), (obj * Type)> ()

    // do not write tag if omitting header or array element
    let omitTag () = (omitHeader && depth = 0) || arrayStack.Peek() = depth - 1

    member this.Get<'a> (ignoreName : bool) (name : string) =
        let key = (name, depth)

        let rec calc () =
            let result = jsonReader.ReadPrimitiveAs<'a> ignoreName name
            match result with
            | SuccessParse v -> v
            | FailParse (name, v, t) ->
                if not <| cache.ContainsKey (name, depth) then
                    cache.Add ((name, depth), (v,t))
                calc ()

        match cache.TryGetValue key with
        | true, (v,_) -> v :?> 'a
        | false, _ -> calc ()

    member __.Cache (ignoreName : bool) (name : string) =
        let calc () =
            let result = jsonReader.ReadPrimitiveAs<'a> ignoreName name
            match result with
            | SuccessParse v ->
                if not <| cache.ContainsKey (name, depth) then
                    cache.Add ((name, depth), (v, v.GetType ()))
            | FailParse (name, v, t) ->
                if not <| cache.ContainsKey (name, depth) then
                    cache.Add ((name, depth), (v,t))
        calc ()


    interface IPickleFormatReader with
            
        member __.BeginReadRoot (tag : string) =
            do jsonReader.Read() |> ignore
                    
            if omitHeader then () else

            if jsonReader.TokenType <> JsonToken.StartObject then raise <| new FormatException("invalid json root object.")
            else
                do jsonReader.MoveNext()
                let version = __.Get<string> false "FsPickler"
                if version <> formatv4000 then
                    let v = Version(version)
                    if version = formatv0960 || version = formatv1200 || version = formatv1400 || version = formatv2000 then
                        raise <| new FormatException(sprintf "JSON format version %O no longer supported." v)
                    else
                        raise <| new FormatException(sprintf "Unrecognized JSON format version %O." v)

                let sTag = __.Get<string> false "type"
                if tag <> sTag then
                    raise <| new InvalidPickleTypeException(tag, sTag)

        member __.EndReadRoot () = 
            if not omitHeader then jsonReader.Read() |> ignore

        member __.BeginReadObject (tag : string) =
            let rec calc () =
                let mutable parse = SuccessParse ""
                if not <| omitTag () then
                    parse <- jsonReader.ReadProperty tag
                    jsonReader.MoveNext ()

                match parse with
                | FailParse p ->
                    match jsonReader.TokenType with
                    | JsonToken.Null
                    | JsonToken.StartArray
                    | JsonToken.StartObject ->
                        failwith "TODO - this needs a new reader!"
                    | _ ->
                        __.Cache (omitTag ()) tag
                        calc ()
                | SuccessParse _ ->
                    if isTopLevelSequence && depth = 0 then
                        arrayStack.Push depth
                        depth <- depth + 1
                        ObjectFlags.IsSequenceHeader

                    else
                        match jsonReader.TokenType with
                        | JsonToken.Null -> ObjectFlags.IsNull
                        | JsonToken.StartArray ->
                            jsonReader.MoveNext()
                            arrayStack.Push depth
                            depth <- depth + 1
                            ObjectFlags.IsSequenceHeader

                        | JsonToken.StartObject ->
                            do jsonReader.MoveNext()
                            depth <- depth + 1

                            if jsonReader.ValueAs<string> () = "_flags" then
                                jsonReader.MoveNext()
                                let csvFlags = jsonReader.ValueAs<string>()
                                jsonReader.MoveNext()
                                parseFlagCsv csvFlags
                            else
                                ObjectFlags.None
                        | token -> raise <| new FormatException(sprintf "expected start of Json object but was '%O'." token)
            calc ()


        member __.EndReadObject () =
            if isTopLevelSequence && depth = 1 then
                arrayStack.Pop () |> ignore
                depth <- depth - 1
                jsonReader.Read() |> ignore
            else
                match jsonReader.TokenType with
                | JsonToken.Null -> ()
                | JsonToken.EndObject -> depth <- depth - 1
                | JsonToken.EndArray ->
                    arrayStack.Pop() |> ignore
                    depth <- depth - 1

                | token -> raise <| new FormatException(sprintf "expected end of JSON object but was '%O'." token)

                if omitHeader && depth = 0 then ()
                else jsonReader.Read() |> ignore

        member __.SerializeUnionCaseNames = true
        member __.UseNamedEnumSerialization = true

        member __.PreferLengthPrefixInSequences = false
        member __.ReadNextSequenceElement () = 
            if isTopLevelSequence && depth = 1 then
                if isJsonDotNet8CustomSequence then
                    // Json.NET 8 handling
                    match jsonReader.TokenType with
                    | JsonToken.None | JsonToken.EndArray 
                    | JsonToken.EndConstructor | JsonToken.EndObject -> false
                    | _ -> 
                        let tr = jsonReader :?> JsonTextReader
                        tr.LinePosition + tr.LineNumber > 0
                else
                    // old logic
                    jsonReader.TokenType <> JsonToken.None
            else
                jsonReader.TokenType <> JsonToken.EndArray

        member __.ReadCachedObjectId () = __.Get<int64> false "id"

        member __.ReadBoolean tag = __.Get<bool> (omitTag ()) tag
        member __.ReadByte tag = __.Get<int64> (omitTag ()) tag |> byte
        member __.ReadSByte tag = __.Get<int64> (omitTag ()) tag |> sbyte

        member __.ReadInt16 tag = __.Get<int64> (omitTag ()) tag |> int16
        member __.ReadInt32 tag = __.Get<int64> (omitTag ()) tag |> int
        member __.ReadInt64 tag = __.Get<int64> (omitTag ()) tag

        member __.ReadUInt16 tag = __.Get<int64> (omitTag ()) tag |> uint16
        member __.ReadUInt32 tag = __.Get<int64> (omitTag ()) tag |> uint32
        member __.ReadUInt64 tag = __.Get<int64> (omitTag ()) tag |> uint64

        member __.ReadSingle tag =
            if not <| omitTag () then
                jsonReader.ReadProperty tag |> ignore //TODO remoe
                jsonReader.MoveNext()

            let value =
                match jsonReader.TokenType with
                | JsonToken.Float -> jsonReader.ValueAs<double> () |> single
                | JsonToken.String -> Single.Parse(jsonReader.ValueAs<string>(), CultureInfo.InvariantCulture)
                | _ -> raise <| new FormatException("not a float.")

            jsonReader.Read() |> ignore
            value
                
        member __.ReadDouble tag =
            if not <| omitTag () then
                jsonReader.ReadProperty tag |> ignore //TODO remoe
                jsonReader.MoveNext()

            let value =
                match jsonReader.TokenType with
                | JsonToken.Float -> jsonReader.ValueAs<double> ()
                | JsonToken.String -> Double.Parse(jsonReader.ValueAs<string>(), CultureInfo.InvariantCulture)
                | _ -> raise <| new FormatException("not a float.")

            jsonReader.Read() |> ignore
            value

        member __.ReadChar tag = let value = __.Get<string> (omitTag ()) tag in value.[0]
        member __.ReadString tag = __.Get<string> (omitTag ()) tag

        member __.ReadBigInteger tag = __.Get<string> (omitTag ()) tag |> BigInteger.Parse

        member __.ReadGuid tag = 
            if isBsonReader then 
                __.Get<Guid> (omitTag ()) tag
            else
                let textGuid = __.Get<string> (omitTag ()) tag 
                new Guid(textGuid)

        member __.ReadTimeSpan tag = __.Get<string> (omitTag ()) tag |> TimeSpan.Parse
        member __.ReadDecimal tag = __.Get<string> (omitTag ()) tag |> decimal

        // BSON spec mandates the use of Unix time; 
        // this has millisecond precision which results in loss of accuracy w.r.t. ticks
        // since the goal of FsPickler is to offer faithful representations of .NET objects
        // we choose to override the spec and serialize ticks outright.
        // see also https://json.codeplex.com/discussions/212067 
        member __.ReadDateTime tag = 
            if isBsonReader then
                if not <| omitTag() then
                    jsonReader.ReadProperty tag |> ignore //TODO remoe
                    jsonReader.MoveNext()

                jsonReader.MoveNext()
                let kind = __.Get<int64> false "kind" |> int |> enum<DateTimeKind>
                let ticks = __.Get<int64> false "ticks"
                if kind = DateTimeKind.Local then
                    let offset = __.Get<int64> false "offset"
                    jsonReader.MoveNext()
                    let dto = new DateTimeOffset(ticks, new TimeSpan(offset))
                    dto.LocalDateTime
                else
                    jsonReader.MoveNext()
                    new DateTime(ticks, kind)
            else
                let dt = __.Get<string> (omitTag ()) tag
                DateTime.Parse(dt, null, DateTimeStyles.RoundtripKind)

        member __.ReadDateTimeOffset tag =
            if isBsonReader then
                if not <| omitTag() then
                    jsonReader.ReadProperty tag |> ignore //TODO remoe
                    jsonReader.MoveNext()

                jsonReader.MoveNext()
                let ticks = __.Get<int64> false "ticks"
                let offset = __.Get<int64> false "offset"
                jsonReader.MoveNext()

                new DateTimeOffset(ticks, new TimeSpan(offset))
            else
                let dt = __.Get<string> (omitTag ()) tag
                DateTimeOffset.Parse(dt, null, DateTimeStyles.RoundtripKind)

        member __.ReadBytes tag =
            if not <| omitTag () then
                jsonReader.ReadProperty tag |> ignore //TODO remoe
                jsonReader.Read() |> ignore

            let bytes =
                if jsonReader.TokenType = JsonToken.Null then null
                elif isBsonReader then jsonReader.ValueAs<byte []> ()
                else
                    let base64 = jsonReader.ValueAs<string> ()
                    Convert.FromBase64String base64

            jsonReader.Read() |> ignore

            bytes

        member __.IsPrimitiveArraySerializationSupported = false
        member __.ReadPrimitiveArray _ _ = raise <| new NotImplementedException()

        member __.Dispose () = 
            if not leaveOpen then jsonReader.Close()