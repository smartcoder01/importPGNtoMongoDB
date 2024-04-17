```markdown
# Chess Game Parser

This project is a Golang for parsing chess games in PGN (Portable Game Notation) format and saving them into a MongoDB database.

## Installation

1. Install Go if it's not already installed. Instructions can be found [here](https://golang.org/doc/install).
2. Install MongoDB if it's not already installed. Instructions can be found [here](https://docs.mongodb.com/manual/installation/).
3. Install dependencies using `go get` command:
   ```sh
   go get github.com/joho/godotenv
   go get go.mongodb.org/mongo-driver/mongo
   ```

## Configuration

The program uses a `.env` file for configuration. Example `.env` file:

```
MONGODB_URI=mongodb://localhost:27017
MONGODB_DATABASE=chess_games
MONGODB_COLLECTION=games
FILE_PATH=path/to/pgn/file.pgn
```

## Usage

To run the program, execute the following command:

```sh
go run main.go
```

The program will read a file containing chess games in PGN format, parse them, and save them into a MongoDB database.

## Data Structure

Each game is saved in MongoDB as a document with the following fields:

- `opening`: opening name
- `eco`: opening code
- `result`: game result
- `white`: white player's name
- `black`: black player's name
- `whiteElo`: white player's Elo rating
- `blackElo`: black player's Elo rating
- `moves`: game moves
- `moves_count`: number of moves
- `event`: event name
- `time_control`: time control
- `termination`: game termination type
- `date`: game date
- `time`: game time
- `site`: game site
