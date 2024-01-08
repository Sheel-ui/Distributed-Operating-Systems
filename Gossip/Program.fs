// Import necessary modules
open Gossip.Utility
open Gossip.Topologies
open Gossip.PushSum
open Gossip.Gossipactor
open Gossip.Counter
open System
open Deedle
open Akka.FSharp

// Entry point of the program
[<EntryPoint>]
let main argv =
    // Create an Akka.NET actor system
    let system = System.create "my-system" (Configuration.load())

    // Define the maximum number of times a node should hear the rumor before stopping transmission
    let maxcnt = 10
    
    // Parse command line arguments for topology, number of nodes, and algorithm
    let topology = argv.[1]
    let numNodes = roundNodes (int argv.[0]) topology
    let algo = argv.[2]
    let path = "results/" + topology + "-" + string numNodes + "-" + algo + ".csv"
    
    
    // Initialize a stopwatch for performance measurement
    let stopTime = Diagnostics.Stopwatch()

    // Create the specified network topology
    let map = buildTopology numNodes topology

    // Spawn the counter actor to track the progress of the algorithm
    let cntRef = spawn system "counter" (counter 0 numNodes path stopTime)

    // Choose and spawn worker actors based on the selected algorithm
    match algo with
    | "pushsum" ->
        // Push Sum Algorithm
        // Initialize all the actors for the push sum algorithm
        let workerRef =
            [ 1 .. numNodes ]
            |> List.map (fun id ->
                let name = "worker" + string id
                (spawn system name (pushSum map id cntRef)))
        // Start the timer
        stopTime.Start()
        // Send the initialization message to all worker actors
        workerRef |> List.iter (fun item -> item <! Initialize)


    | "gossip" ->
        // Gossip Algorithm
        // Create the desired number of worker actors and randomly select one to start the gossip algorithm
        let workerRef =
            [ 1 .. numNodes ]
            |> List.map (fun id ->
                let name = "worker" + string id
                spawn system name (gossip maxcnt map id cntRef))
            |> chooseRandom
        // Start the timer
        stopTime.Start()
        // Send the initial message to initiate the gossip algorithm
        workerRef <! "heardRumor"

   
    // Wait until all actors are terminated
    system.WhenTerminated.Wait()
    0 // return an integer exit code
    // Each actor maintains a flag to describe its active state
