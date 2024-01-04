
To run the projects:
    Distributed Grep: go run main.go helpers.go  
    Membership List: start normal node with go run main.go
    Distributed Computing: go run main.go mapleJuice.go rpcHandler.go consistantHashing.go
         flags:
            -intro: run as introducer (*defauls to vm7)
            -verbose: print debug statements
            -bps: print bandwidth (bytes/sec)
            -fp: print false positive rates per minute
            -leader: run node as leader


To run the unit tests:
    Distributed Grep: go run UnitTest.go helpers.go


Distributed Grep Details: 
    The grep command is as follows:
        grep [options] pattern 
    Example: 
        grep -ce stars

To run unit tests, we need all the log files locally to compare a local grep with the distributed grep result.

