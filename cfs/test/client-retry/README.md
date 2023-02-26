TODO

# Folder structure

client-retry
|
|__sanity
    |
    |__no-crash
        |
        |__commands
        |  
        |__client
        |  
        |__server
    |
    |__crash TODO

# Enumerate tests
## sanity/no-crash
- Tests the sanity of using inode instead of fd, client retrying idempotent ops.
- Right now we are not checking for durability. Maybe in the future?
- Test for CRUD ops, read metadata ops (stat, etc), inode reassignment and simple read-write. Commands used are present in the commands dir. 

### Naming conventions
- All client scripts are in the client dir.
- All server scripts are in the server dir.
- Commands used by the client are present in the commands dir. 

### How to run
- Test 1 (Single client, Single server):
On a terminal, run `sudo ./run-server-1c1s.sh`. On another terminal run `sudo ./run-client-1c1s.sh`.
- Test 2 (Single client, Multi server):
On a terminal, run `sudo ./run-server-1c2s.sh`. On another terminal run `sudo ./run-client-1c1s.sh`.
- Test 3 (Multi client, Single server):
On a terminal, run `sudo ./run-server-2c1s.sh`. On another terminal run `sudo ./run-client-1c1s.sh`. Enter 7,8.
- Test 4 (Multi client, Multi server):
On a terminal, run `sudo ./run-server-2c2s.sh`. On another terminal run `sudo ./run-client-1c1s.sh`. Enter 7,9 and 8,10.