# CSV to PSQL Concurrent Copy Tool pgx_multiLoad

This tool allows you to concurrently copy multiple CSV files into a PostgreSQL (PSQL) database using goroutines for parallel processing and the pgx library for database interactions.

## Installation

1. Make sure you have Go installed on your system. If not, you can download it from the official website: https://golang.org/dl/

2. Get the tool using `go get` command:

   ```bash
   go get github.com/stock-jarvis/pgx_multiLoad
   ```

3. Change directory to the tool's source code:

   ```bash
   cd $GOPATH/src/github.com/stock-jarvis/pgx_multiLoad
   ```

4. Build the tool:

   ```bash
   go build
   ```

## Usage

The tool provides several flags for configuring the CSV to PSQL copying process. Below are the available flags and their usages:

- `-s`: Source Files Directory (required)
  - Specifies the directory path containing the CSV files that you want to copy into the PSQL database.

- `-h`: Hostname for destination PSQL (required)
  - Specifies the hostname of the PostgreSQL server where you want to copy the data.

- `-d`: Database for destination PSQL (required)
  - Specifies the name of the PostgreSQL database where you want to store the copied data.

- `-p`: Password for destination PSQL user postgres (required)
  - Provides the password for the PostgreSQL user "postgres" to access the database.

- `-e`: Exchange for Fill (optional)
  - Specifies the exchange for fill information, if applicable.

- `-threads`: Number of Threads for Parallel Processing (optional, default=20)
  - Sets the number of goroutines (threads) to be used for concurrent processing. Increasing this value can speed up the copying process, but it should not exceed the system's capabilities.

## Example

To use the tool, run the binary with the required flags:

```bash
./pgx_multiLoad -s /path/to/source/csv_files -h localhost -d mydatabase -p mysecretpassword -e NYSE -threads 10
```

Please note that the actual path to the binary and the flags' values should be adjusted according to your system and requirements.

## License

This tool is distributed under the MIT License. See the `LICENSE` file for more information.

## Contributing

Feel free to contribute to this project by creating issues or submitting pull requests on the GitHub repository: https://github.com/stock-jarvis/pgx_multiLoad

Your contributions are highly appreciated!

## Acknowledgments

This tool was made possible with the use of the following open-source libraries:

- [pgx](https://github.com/jackc/pgx): PostgreSQL driver and toolkit for Go.
- [Go standard library](https://golang.org/pkg/): Various packages from the Go standard library.

---

Please make sure to include relevant information in the examples section when you update the README with your specific use cases. Happy coding! If you have any questions or need further assistance, feel free to reach out.
