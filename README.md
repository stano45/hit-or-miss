# Hit or Miss: A Rust Distributed Key-Value Store

Hit or Miss is a simple distributed key-value store implemented in Rust. This project consists of two servers: a master server and a partition server. The master server is responsible for managing connections and forwarding requests to the appropriate partition server. The partition servers store the data and process the commands sent by the master.

## Features

- Simple and efficient command parsing
- LRU Cache for key-value storage on partition servers
- Consistent hashing to distribute data across multiple partition servers
- Support for \`GET\`, \`SET\`, and \`DELETE\` operations
- Automatic partition server registration and load balancing

## System design sketch
![Screenshot from 2023-03-30 10-31-41](https://user-images.githubusercontent.com/17514555/228778919-c5573eb7-3f4b-495a-9a54-c00f9b27b946.png)

## Prerequisites

To run Hit or Miss, you need to have Rust and Cargo installed. Visit [the official Rust website](https://www.rust-lang.org/tools/install) for installation instructions.

## Running the Project

1. Clone the repository:

   ```
   git clone https://github.com/stano45/hit-or-miss.git
   cd hit-or-miss
   ```

2. Start the master server:

   ```
   cargo run --bin master
   ```

   The master server will start listening on the default address \`127.0.0.1:6969\`.

3. Start one or more partition servers in separate terminal windows or tabs:

   ```
   cargo run --bin partition
   ```

   The partition servers will automatically connect to the master server and register themselves.

4. Use a client (e.g., telnet, netcat, or a custom client) to connect to the master server and send commands.

   For example:
   ```
   echo "SET key value" | nc 127.0.0.1 6969
   ```

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
