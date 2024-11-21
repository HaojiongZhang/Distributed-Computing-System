```markdown
# Distributed Computing System

This project is a distributed computing system modeled after Hadoop with SQL capabilities. It uses MapReduce and includes an HDFS-like distributed file system.

## How to Run the Project

### Distributed Grep
```bash
go run main.go helpers.go
```

### Membership List
Start a normal node with:
```bash
go run main.go
```

### Distributed Computing
Run with:
```bash
go run main.go mapleJuice.go rpcHandler.go consistantHashing.go
```

#### Flags:
- `-intro` : Run as introducer (defaults to `vm7`).
- `-verbose` : Print debug statements.
- `-bps` : Print bandwidth (bytes/second).
- `-fp` : Print false positive rates per minute.
- `-leader` : Run the node as leader.

## Running Unit Tests

### Distributed Grep Unit Tests
Run with:
```bash
go run UnitTest.go helpers.go
```

To run unit tests, ensure all log files are available locally. These are used to compare the results of a local `grep` with the distributed `grep`.

## Distributed Grep Details

The distributed `grep` command supports the following syntax:
```bash
grep [options] pattern
```

#### Example:
```bash
grep -ce stars
```
```
