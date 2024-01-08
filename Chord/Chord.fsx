module ChordUtils
open System
open Akka.FSharp
open System.Threading

module Tracking = 
    // Message for Tracking Hops
    type HopsTrackingMessage =
        | RecordHops of int * int * int
        
    // trackHops takes numNodes (an integer) and a mailbox for the actor
    let trackHops numNodes (mailbox: Actor<_>) =
        // Initializing mutable variables for tracking
        let mutable completedNodeCount = 0
        let mutable totalHopCount = 0
        let mutable totalRequests = 0

        // Defining a recursive loop function
        let rec loop () = actor {
            // Wait for a message to be received
            let! message = mailbox.Receive ()

            // Match the received message with its pattern
            match message with
            | RecordHops (nodeID, hops, requests) ->
                // Printing message details
                printfn "Node with ID %d completed with %d requests in %d hops" nodeID  requests hops
                // Update counters
                completedNodeCount <- completedNodeCount + 1
                totalHopCount <- totalHopCount + hops
                totalRequests <- totalRequests + requests
                
                // Check if all nodes have completed
                if (completedNodeCount = numNodes) then
                    // If all nodes have completed, print the statistics and terminate the system
                    printfn "Total Hops: %d" totalHopCount
                    printfn "Total Requests: %d" totalRequests
                    printfn "Average Hops: %f" ((float totalHopCount) / (float totalRequests))
                    mailbox.Context.System.Terminate() |> ignore

                // Continue processing messages by recursively calling the loop function
                return! loop ()
        }
        // Start the loop function to begin processing messages
        loop ()


   
module NodeFunctions = 
    type NodeMessage =
        | Create
        | Join of int
        | SearchPredecessorNode
        | ReceivePredecessorNode of int
        | SearchSuccessorNode of int
        | ReceiveSuccessorNode of int
        | SearchKeySuccessor of int * int * int
        | KeyFound of int
        | StartQueries
        | QueryRequest
        | StabilizeNetwork
        | FixFingerTable
        | SearchFingerTableSuccessor of int * int * int
        | UpdateFingerEntry of int * int   
        | Notify of int

    let getActorPath s =
        // Create a string 'actorPath' by concatenating a fixed string with the string representation of 's'
        let actorPath = @"akka://my-system/user/" + string s
        // Return the constructed actor path
        actorPath

    // Function to check if 'value' is between 'start' and 'end', without including 'start' and 'end'
    let inBetweenWithoutStartWithoutEnd hashSpace start value endValue =
        let correctedEndValue = if (endValue < start) then endValue + hashSpace else endValue
        let correctedValue = if ((value < start) && (start > endValue)) then (value + hashSpace) else value
        (start = endValue) || ((correctedValue > start) && (correctedValue < correctedEndValue))

    // Function to check if 'value' is between 'start' and 'end', without including 'start', but including 'end'
    let inBetweenWithoutStartWithEnd hashSpace start value endValue =
        let correctedEndValue = if (endValue < start) then endValue + hashSpace else endValue
        let correctedValue = if ((value < start) && (start > endValue)) then (value + hashSpace) else value
        (start = endValue) || ((correctedValue > start) && (correctedValue <= correctedEndValue))

    let createNode (nodeID: int) m maxRequests hopCounterRef (mailbox: Actor<_>) =
        printfn "Creating node %d" nodeID
        let hashSpace = int (Math.Pow(2.0, float m))
        let mutable predecessorID = -1
        let mutable fingerTable = Array.create m -1
        let mutable next = 0
        let mutable totalHopCount = 0
        let mutable numRequests = 0

        let rec loop () = actor {
            // Wait for a message to be received
            let! message = mailbox.Receive ()

            // Get the sender of the message
            let sender = mailbox.Sender ()

            // Match the received message with its pattern
            match message with
            | Create ->
                // Initialization when a new node is created
                predecessorID <- -1
                for i = 0 to m - 1 do
                    fingerTable.[i] <- nodeID

                // Schedule periodic tasks for stabilization and fixing finger table
                mailbox.Context.System.Scheduler.ScheduleTellRepeatedly (
                    TimeSpan.FromMilliseconds(0.0),
                    TimeSpan.FromMilliseconds(500.0),
                    mailbox.Self,
                    StabilizeNetwork
                )
                mailbox.Context.System.Scheduler.ScheduleTellRepeatedly (
                        TimeSpan.FromMilliseconds(0.0),
                        TimeSpan.FromMilliseconds(500.0),
                        mailbox.Self,
                        FixFingerTable
                    )

            | Join (nDash) ->
                // When a node is joining the network
                // Reset predecessorID to -1 to ensure it is updated during the process
                predecessorID <- -1

                // Get the actor path for the joining node
                let nDashPath = getActorPath nDash

                // Create a reference to the joining node
                let nDashRef = mailbox.Context.ActorSelection nDashPath

                // Send a message to the joining node to initiate the process
                nDashRef <! SearchSuccessorNode (nodeID)

            | SearchPredecessorNode ->
                // Message to request the predecessor node
                sender <! ReceivePredecessorNode (predecessorID)

            | ReceivePredecessorNode (x) ->
                // Received information about a potential predecessor node
                if ((x <> -1) && (inBetweenWithoutStartWithoutEnd hashSpace nodeID x fingerTable.[0])) then
                    // Update the finger table with the new predecessor if it's within the range
                    fingerTable.[0] <- x
                
                // Get the ID of the current successor
                let successorID = fingerTable.[0]

                // Get the actor path of the successor node
                let successorPath = getActorPath successorID

                // Create a reference to the successor node
                let successorRef = mailbox.Context.ActorSelection successorPath

                // Notify the successor about the current node
                successorRef <! Notify (nodeID)


            | ReceiveSuccessorNode (successorID) ->
                // Received information about the successor node

                // Update the entire finger table with the new successor ID
                for index = 0 to m - 1 do
                    fingerTable.[index] <- successorID

                // Schedule regular tasks to stabilize the network and fix finger tables
                mailbox.Context.System.Scheduler.ScheduleTellRepeatedly (
                    TimeSpan.FromMilliseconds(0.0),
                    TimeSpan.FromMilliseconds(500.0),
                    mailbox.Self,
                    StabilizeNetwork
                )

                mailbox.Context.System.Scheduler.ScheduleTellRepeatedly (
                    TimeSpan.FromMilliseconds(0.0),
                    TimeSpan.FromMilliseconds(500.0),
                    mailbox.Self,
                    FixFingerTable
                )


            | SearchSuccessorNode (id) ->
                // Attempting to find the successor node for a given ID

                if (inBetweenWithoutStartWithEnd hashSpace nodeID id fingerTable.[0]) then
                    // If the ID is within the range of the immediate successor
                    let newNodePath = getActorPath id
                    let newNodeRef = mailbox.Context.ActorSelection newNodePath
                    newNodeRef <! ReceiveSuccessorNode (fingerTable.[0])
                else
                    // If the ID doesn't belong to the immediate successor
                    let mutable i = m - 1
                    while (i >= 0) do
                        if (inBetweenWithoutStartWithoutEnd hashSpace nodeID fingerTable.[i] id) then
                            // If the ID is within the range of a finger table entry
                            let closestPrecedingNodeID = fingerTable.[i]
                            let closestPrecedingNodePath = getActorPath closestPrecedingNodeID
                            let closestPrecedingNodeRef = mailbox.Context.ActorSelection closestPrecedingNodePath
                            closestPrecedingNodeRef <! SearchSuccessorNode (id)
                            i <- -1
                        i <- i - 1

            | StabilizeNetwork ->
                // Initiating the network stabilization process

                let successorID = fingerTable.[0]
                let successorPath = getActorPath successorID
                let successorRef = mailbox.Context.ActorSelection successorPath
                successorRef <! SearchPredecessorNode

            | SearchFingerTableSuccessor (originNodeID, next, id) ->
                // Searching for the successor of a given ID in the finger table

                if(inBetweenWithoutStartWithEnd hashSpace nodeID id fingerTable.[0]) then
                    // If the ID is within the range of the immediate successor
                    let originNodePath = getActorPath originNodeID
                    let originNodeRef = mailbox.Context.ActorSelection originNodePath
                    originNodeRef <! UpdateFingerEntry (next, fingerTable.[0])
                else
                    // If the ID doesn't belong to the immediate successor
                    let mutable i = m - 1
                    while(i >= 0) do
                        if(inBetweenWithoutStartWithoutEnd hashSpace nodeID fingerTable.[i] id) then
                            // If the ID is within the range of a finger table entry
                            let closestPrecedingNodeID = fingerTable.[i]
                            let closestPrecedingNodePath = getActorPath closestPrecedingNodeID
                            let closestPrecedingNodeRef = mailbox.Context.ActorSelection closestPrecedingNodePath
                            closestPrecedingNodeRef <! SearchFingerTableSuccessor (originNodeID, next, id)
                            i <- -1
                        i <- i - 1

            // Update the finger table entry
            | UpdateFingerEntry (next, fingerSuccessor) ->
                fingerTable.[next] <- fingerSuccessor

            // Notify the node's predecessor or set a new predecessor
            | Notify (nDash) ->
                if((predecessorID = -1) || (inBetweenWithoutStartWithoutEnd hashSpace predecessorID nDash nodeID)) then
                    predecessorID <- nDash

            // Fix the finger table
            | FixFingerTable ->
                // Move to the next finger
                next <- next + 1
                if(next >= m) then
                    next <- 0

                // Calculate the value of the next finger
                let fingerValue = nodeID + int (Math.Pow(2.0, float (next)))
                
                // Search for the successor of the next finger
                mailbox.Self <! SearchFingerTableSuccessor (nodeID, next, fingerValue)

            // Handle the case when a key is found
            | KeyFound (hopCount) ->
                if(numRequests < maxRequests) then
                    // Update statistics for hops and requests
                    totalHopCount <- totalHopCount + hopCount
                    numRequests <- numRequests + 1


            | StartQueries ->
                if(numRequests < maxRequests) then
                    mailbox.Self <! QueryRequest
                    mailbox.Context.System.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(1.), mailbox.Self, StartQueries)
                else
                    // Done querying. Send its current status to the hop counter
                    hopCounterRef <! Tracking.RecordHops (nodeID, totalHopCount, numRequests)

            | QueryRequest ->
                let key = (System.Random()).Next(hashSpace)
                mailbox.Self <! SearchKeySuccessor (nodeID, key, 0)

            //Scalable Key Lookup
            | SearchKeySuccessor (originNodeID, id, numHops) ->
                // Case 1: The current node is the key holder
                if(id = nodeID) then
                    // Notify the origin node that the key is found
                    let originNodePath = getActorPath originNodeID
                    let originNodeRef = mailbox.Context.ActorSelection originNodePath
                    originNodeRef <! KeyFound (numHops)

                // Case 2: The key is within the current node's range
                elif(inBetweenWithoutStartWithEnd hashSpace nodeID id fingerTable.[0]) then
                    // Notify the origin node that the key is found
                    let originNodePath = getActorPath originNodeID
                    let originNodeRef = mailbox.Context.ActorSelection originNodePath
                    originNodeRef <! KeyFound (numHops)

                // Case 3: The current node forwards the request to its finger table entries
                else
                    let mutable i = m - 1
                    while(i >= 0) do
                        // Find the closest preceding node for the key
                        if(inBetweenWithoutStartWithoutEnd hashSpace nodeID fingerTable.[i] id) then
                            let closestPrecedingNodeID = fingerTable.[i]
                            let closestPrecedingNodePath = getActorPath closestPrecedingNodeID
                            let closestPrecedingNodeRef = mailbox.Context.ActorSelection closestPrecedingNodePath
                            closestPrecedingNodeRef <! SearchKeySuccessor (originNodeID, id, numHops + 1)
                            i <- -1
                        i <- i - 1

            // //Simple key lookup
            // | SearchKeySuccessor (originNodeID, id, numHops) ->
            //     if(id = nodeID) then
            //         //printfn "Key %d is at the node %d in %d hops" id nodeID numHops
            //         let originNodePath = getActorPath originNodeID
            //         let originNodeRef = mailbox.Context.ActorSelection originNodePath
            //         originNodeRef <! KeyFound (numHops)
            //     elif(inBetweenWithoutStartWithEnd hashSpace nodeID id fingerTable.[0]) then
            //         //printfn "Key %d is at the node %d in %d hops" id fingerTable.[0] numHops
            //         let originNodePath = getActorPath originNodeID
            //         let originNodeRef = mailbox.Context.ActorSelection originNodePath
            //         originNodeRef <! KeyFound (numHops)
            //     else
            //         let successorID = fingerTable.[0]
            //         let successorPath = getActorPath successorID
            //         let successorRef = mailbox.Context.ActorSelection successorPath
            //         successorRef <! SearchKeySuccessor (originNodeID, id, numHops + 1)
            return! loop ()
        }
        loop ()