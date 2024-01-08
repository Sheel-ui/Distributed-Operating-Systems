module Gossip
open System
open Deedle
open Akka.FSharp

module Utility = 
    // Function to round the number of nodes based on topology
    let roundNodes numNodes topology =
        match topology with
        | "2d"
        | "imperfect2d" -> Math.Pow (Math.Round (sqrt (float numNodes)), 2.0) |> int
        | "3d"
        | "imperfect3d" -> Math.Pow (Math.Round ((float numNodes) ** (1.0 / 3.0)), 3.0)  |> int
        | _ -> numNodes

    // Function to pick a random element from a list
    let chooseRandom (l: List<_>) =
        let r = Random()
        l.[r.Next(l.Length)]

    // Function to get a random neighbor ID from the topology map
    let getRandomNeighbor (topologyMap: Map<_, _>) id =
        let random = Random()
        let (neighborList: List<_>) = (topologyMap.TryFind id).Value
        neighborList.[random.Next(neighborList.Length)]

module Topologies = 
    // Function to build a line topology
    let lineTopology numNodes =
        let mutable map = Map.empty
        [ 1 .. numNodes ]
        |> List.map (fun id ->
            let listNeighbors = List.filter (fun y -> (y = id + 1 || y = id - 1)) [ 1 .. numNodes ]
            map <- map.Add(id, listNeighbors))
        |> ignore
        map

    // Function to get 2D grid neighbors for a node
    let gridNeighbors2D id numNodes =
        let mutable map = Map.empty
        let lenSide = sqrt (float numNodes) |> int
        [ 1 .. numNodes ]
        |> List.filter (fun y ->
            if (id % lenSide = 0) then (y = id - 1 || y = id - lenSide || y = id + lenSide)
            elif (id % lenSide = 1) then (y = id + 1 || y = id - lenSide || y = id + lenSide)
            else (y = id - 1 || y = id + 1 || y = id - lenSide || y = id + lenSide))

    // Function to build an imperfect 2D grid topology
    let buildImperfect2DTopology numNodes =
        let mutable map = Map.empty
        [ 1 .. numNodes ]
        |> List.map (fun id ->
            let mutable listNeighbors = gridNeighbors2D id numNodes
            let random =
                [ 1 .. numNodes ]
                |> List.filter (fun m -> m <> id && not (listNeighbors |> List.contains m))
                |> Utility.chooseRandom
            let listNeighbors = random :: listNeighbors
            map <- map.Add(id, listNeighbors))
        |> ignore
        map

    // Function to build a 2D grid topology
    let build2DTopology numNodes =
        let mutable map = Map.empty
        [ 1 .. numNodes ]
        |> List.map (fun id ->
            let listNeighbors = gridNeighbors2D id numNodes
            map <- map.Add(id, listNeighbors))
        |> ignore
        map

    
    // Function to get 3D grid neighbors for a node
    let gridNeighbors3D id numNodes =
        let lenSide = Math.Round(Math.Pow((float numNodes), (1.0 / 3.0))) |> int
        [ 1 .. numNodes ]
        |> List.filter (fun y ->
            // Check conditions based on 3D grid topology
            if (id % lenSide = 0) then
                if (id % (int (float (lenSide) ** 2.0)) = 0) then
                    (y = id - 1 || y = id - lenSide || y = id - int ((float (lenSide) ** 2.0)) || y = id + int ((float (lenSide) ** 2.0)))
                elif (id % (int (float (lenSide) ** 2.0)) = lenSide) then
                    (y = id - 1 || y = id + lenSide || y = id - int ((float (lenSide) ** 2.0)) || y = id + int ((float (lenSide) ** 2.0)))
                else
                    (y = id - 1 || y = id - lenSide || y = id + lenSide || y = id - int ((float (lenSide) ** 2.0)) || y = id + int ((float (lenSide) ** 2.0)))        
            elif (id % lenSide = 1) then
                if (id % (int (float (lenSide) ** 2.0)) = 1) then
                    (y = id + 1 || y = id + lenSide || y = id - int ((float (lenSide) ** 2.0)) || y = id + int ((float (lenSide) ** 2.0)))
                elif (id % (int (float (lenSide) ** 2.0)) = int (float (lenSide) ** 2.0) - lenSide + 1 ) then
                    (y = id + 1 || y = id - lenSide || y = id - int ((float (lenSide) ** 2.0)) || y = id + int ((float (lenSide) ** 2.0)))
                else
                    (y = id + 1 || y = id - lenSide || y = id + lenSide || y = id - int ((float (lenSide) ** 2.0)) || y = id + int ((float (lenSide) ** 2.0)))
            elif (id % (int (float (lenSide) ** 2.0)) > 1) && (id % (int (float (lenSide) ** 2.0)) < lenSide) then
                (y = id - 1 || y = id + 1 || y = id + lenSide || y = id - int ((float (lenSide) ** 2.0)) || y = id + int ((float (lenSide) ** 2.0)))
            elif (id % (int (float (lenSide) ** 2.0)) > int (float (lenSide) ** 2.0) - lenSide + 1) && (id % (int (float (lenSide) ** 2.0)) < (int (float (lenSide) ** 2.0))) then
                (y = id - 1 || y = id + 1 || y = id - lenSide || y = id - int ((float (lenSide) ** 2.0)) || y = id + int ((float (lenSide) ** 2.0)))
            else
                (y = id - 1 || y = id + 1 || y = id - lenSide || y = id + lenSide || y = id - int ((float (lenSide) ** 2.0)) || y = id + int ((float (lenSide) ** 2.0))))

    // Function to build an imperfect 3D grid topology
    let buildImperfect3DTopology numNodes =
        let mutable map = Map.empty
        [ 1 .. numNodes ]
        |> List.map (fun id ->
            let mutable listNeighbors = gridNeighbors3D id numNodes
            let random =
                [ 1 .. numNodes ]
                |> List.filter (fun m -> m <> id && not (listNeighbors |> List.contains m))
                |> Utility.chooseRandom
            let listNeighbors = random :: listNeighbors
            map <- map.Add(id, listNeighbors))
        |> ignore
        map

    // Function to build a 3D grid topology
    let build3DTopology numNodes =
        let mutable map = Map.empty
        [ 1 .. numNodes ]
        |> List.map (fun id ->
            let listNeighbors = gridNeighbors3D id numNodes
            map <- map.Add(id, listNeighbors))
        |> ignore
        map

    // Function to build a full (complete) topology
    let fullTopology numNodes =
        let mutable map = Map.empty
        [ 1 .. numNodes ]
        |> List.map (fun id ->
            let listNeighbors = List.filter (fun y -> id <> y) [ 1 .. numNodes ]
            map <- map.Add(id, listNeighbors))
        |> ignore
        map

    // Function to build a topology based on specified type
    let buildTopology numNodes topology =
        let mutable map = Map.empty
        match topology with
        | "line" -> lineTopology numNodes
        | "2d" -> build2DTopology numNodes
        | "imperfect2d" -> buildImperfect2DTopology numNodes
        | "3d" -> build3DTopology numNodes
        | "imperfect3d" -> buildImperfect3DTopology numNodes
        | "full" -> fullTopology numNodes

module Counter = 
    // Define messages for the counter actor
    type cntMsg =
        | GossipConverge
        | PushSumConverge of int * float
    // Define the result record for counting convergence
    type Result = { nodesConverged: int; TimeElapsed: int64; }

    // Function to define the behavior of the counter actor
    let counter initialCount numNodes (filepath: string) (stopTime: Diagnostics.Stopwatch) (mailbox: Actor<'a>) =
        let rec loop count (dfList: Result list) =
            actor {
                let! message = mailbox.Receive()
                match message with
                | PushSumConverge (id, avg) ->
                    let newRecord = { nodesConverged = count + 1; TimeElapsed = stopTime.ElapsedMilliseconds }
                    if (count + 1 = numNodes) then
                        stopTime.Stop()
                        printfn "Push Sum Algorithm has converged in %d ms" stopTime.ElapsedMilliseconds
                        let df = Frame.ofRecords dfList
                        mailbox.Context.System.Terminate() |> ignore
                    return! loop (count + 1) (List.append dfList [newRecord])

                | GossipConverge ->
                    let newRecord = { nodesConverged = count + 1; TimeElapsed = stopTime.ElapsedMilliseconds; }
                    if (count + 1 = numNodes) then
                        stopTime.Stop()
                        printfn "Gossip Algorithm has converged in %d ms" stopTime.ElapsedMilliseconds
                        let df = Frame.ofRecords dfList
                        mailbox.Context.System.Terminate() |> ignore
                    return! loop (count + 1) (List.append dfList [newRecord])
                
            }
        loop initialCount []

module Gossipactor = 
    let gossip maxcnt (topologyMap: Map<_, _>) id cntRef (mailbox: Actor<_>) = 
        let rec loop (count: int) = actor {
            let! message = mailbox.Receive ()
            match message with
            | "heardRumor" ->
                if count = 0 then
                    // Schedule spreading rumor after a delay
                    mailbox.Context.System.Scheduler.ScheduleTellOnce(
                        TimeSpan.FromMilliseconds(25.0),
                        mailbox.Self,
                        "spreadRumor"
                    )
                    // Notify the counter about GossipConverge
                    cntRef <! Counter.GossipConverge
                    return! loop (count + 1)
                else
                    return! loop (count + 1)
            | "spreadRumor" ->
                if count >= maxcnt then
                    return! loop count
                else
                    // Spread rumor to a random neighbor after a delay
                    let neighborID = Utility.getRandomNeighbor topologyMap id
                    let neighborPath = @"akka://my-system/user/worker" + string neighborID
                    let neighborRef = mailbox.Context.ActorSelection(neighborPath)
                    neighborRef <! "heardRumor"
                    mailbox.Context.System.Scheduler.ScheduleTellOnce(
                        TimeSpan.FromMilliseconds(25.0),
                        mailbox.Self,
                        "spreadRumor"
                    )
                    return! loop count
            | _ ->
                printfn "[INFO] Node %d has received unhandled message" id
                return! loop count
        }
        loop 0

module PushSum = 
    type PushSumMessage =
    | Initialize
    | Message of float * float
    | Round

    let pushSum (topologyMap: Map<_, _>) id cntRef (mailbox: Actor<_>) = 
        let rec loop sNode wNode sSum wSum count isTransmitting = actor {
            if isTransmitting then
                let! message = mailbox.Receive ()
                match message with
                | Initialize ->
                    // Initialize the Push-Sum algorithm
                    mailbox.Self <! Message (float id, 1.0)
                    // Schedule repeated rounds of the algorithm
                    mailbox.Context.System.Scheduler.ScheduleTellRepeatedly (
                        TimeSpan.FromMilliseconds(0.0),
                        TimeSpan.FromMilliseconds(25.0),
                        mailbox.Self,
                        Round
                    )
                    return! loop (float id) 1.0 0.0 0.0 0 isTransmitting
                | Message (s, w) ->
                    // Process incoming messages in the Push-Sum algorithm
                    return! loop sNode wNode (sSum + s) (wSum + w) count isTransmitting
                | Round ->
                    // Perform a round of the Push-Sum algorithm
                    let neighborID = Utility.getRandomNeighbor topologyMap id
                    let neighborPath = @"akka://my-system/user/worker" + string neighborID
                    let neighborRef = mailbox.Context.ActorSelection(neighborPath)
                    // Transmit and receive messages with a neighbor
                    mailbox.Self <! Message (sSum / 2.0, wSum / 2.0)
                    neighborRef <! Message (sSum / 2.0, wSum / 2.0)

                            // Check for convergence
                    if(abs ((sSum / wSum) - (sNode / wNode)) < 1.0e-10) then
                        let newCount = count + 1
                        if newCount = 10 then

                            // Notify the counter about PushSumConverge
                            cntRef <! Counter.PushSumConverge (id, sSum / wSum)
                            return! loop sSum wSum 0.0 0.0 newCount false
                        else
                            return! loop (sSum / 2.0) (wSum / 2.0) 0.0 0.0 newCount isTransmitting 
                    else
                        return! loop (sSum / 2.0) (wSum / 2.0) 0.0 0.0 0 isTransmitting
        }
        loop (float id) 1.0 0.0 0.0 0 true
