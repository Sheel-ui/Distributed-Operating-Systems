open ChordUtils.NodeFunctions
open ChordUtils.Tracking
open System
open Akka.FSharp
open System.Threading

[<EntryPoint>]
let main argv =
    // Create system
    let system = System.create "my-system" (Configuration.load())

    // Parse command line arguments
    let numNodes = int argv.[0]
    let numRequests = int argv.[1]

    // m-bit identifier
    let m = 20
    let hashSpace = int (Math.Pow(2.0, float m))

    // Spawn hopCounter
    let hopCounterRef = spawn system "hopCounter" (trackHops numNodes)

    // Spawn nodes
    let nodeIDs = Array.create numNodes -1;
    let nodeRefs = Array.create numNodes null;
    let mutable i = 0;
    while(i < numNodes) do
        try
            let nodeID  = (Random()).Next(hashSpace)
            nodeIDs.[i] <- nodeID
            nodeRefs.[i] <- spawn system (string nodeID) (createNode nodeID m numRequests hopCounterRef)
            if(i = 0) then
                nodeRefs.[i] <! Create
            else
                nodeRefs.[i] <! Join(nodeIDs.[0])
            i <- i + 1
            Thread.Sleep(500)
        with _ -> ()

    // Wait for some time to get system stabilized
    printfn "Waiting for 30 sec for system stabalization"
    Thread.Sleep(30000)
    // Start querying
    for nodeRef in nodeRefs do
        nodeRef <! StartQueries
        Thread.Sleep(500)

    // Wait till all the actors are terminated
    system.WhenTerminated.Wait()
    0 // return an integer exit code