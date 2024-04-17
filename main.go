package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Game struct represents a chess game
type Game struct {
	Opening     string `bson:"opening"`
	Eco         string `bson:"eco"`
	Result      string `bson:"result"`
	White       string `bson:"white"`
	Black       string `bson:"black"`
	WhiteElo    int    `bson:"whiteElo"`
	BlackElo    int    `bson:"blackElo"`
	Moves       string `bson:"moves"`
	MovesCount  int    `bson:"moves_count"`
	Event       string `bson:"event"`
	TimeControl string `bson:"time_control"`
	Termination string `bson:"termination"`
	Date        string `bson:"date"`
	Time        string `bson:"time"`
	Site        string `bson:"site"`
}

func main() {
	if err := godotenv.Load(); err != nil {
		fmt.Println("No .env file found")
	}

	// get .env params
	mongoUri := os.Getenv("MONGODB_URI")
	mongoDatabase := os.Getenv("MONGODB_DATABASE")
	mongoCollection := os.Getenv("MONGODB_COLLECTION")

	// Folder Path with Games
	folderPath := os.Getenv("FOLDER_PATH")

	// MongoDB Client
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(mongoUri))
	if err != nil {
		fmt.Println("Failed to connect to MongoDB:", err)
		return
	}
	defer client.Disconnect(context.Background())

	// Collection
	collection := client.Database(mongoDatabase).Collection(mongoCollection)

	// Process files in the folder concurrently
	var wg sync.WaitGroup
	var mutex sync.Mutex
	var totalGames int

	err = filepath.Walk(folderPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			fmt.Printf("Error accessing file %s: %s\n", path, err)
			return nil
		}

		if info.IsDir() {
			// Skip directories
			return nil
		}

		wg.Add(1)
		go func(filePath string) {
			defer wg.Done()
			processFile(filePath, collection, &totalGames, &mutex)
		}(path)

		return nil
	})
	if err != nil {
		fmt.Println("Error processing files:", err)
	}

	wg.Wait()

	fmt.Printf("Finished. Total Games: %d\n", totalGames)
}

func processFile(filePath string, collection *mongo.Collection, totalProcessed *int, mutex *sync.Mutex) int {
	// Read file
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("Failed to open file %s: %s\n", filePath, err)
		return 0
	}
	defer file.Close()

	// Create scanner with buffer
	scanner := bufio.NewScanner(file)

	// Make buffer
	var gameData strings.Builder
	var gamesProcessed int

	// Start Parsing
	for scanner.Scan() {
		line := scanner.Text()

		if strings.HasPrefix(line, "[Event ") {
			// Start New Game
			if gameData.Len() > 0 {
				processGame(gameData.String(), collection, totalProcessed, mutex)
				gamesProcessed++
				gameData.Reset()
			}
		}

		gameData.WriteString(line + "\n")
	}

	// Processing Last game
	if gameData.Len() > 0 {
		processGame(gameData.String(), collection, totalProcessed, mutex)
		gamesProcessed++
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading file %s: %s\n", filePath, err)
	}

	return gamesProcessed
}

func processGame(data string, collection *mongo.Collection, totalProcessed *int, mutex *sync.Mutex) {
	game := parseGame(data)

	// Import to MongoDB
	_, err := collection.InsertOne(context.Background(), game)
	if err != nil {
		fmt.Println("Failed to insert game into MongoDB:", err)
		return
	}

	mutex.Lock()
	*totalProcessed++
	fmt.Printf("Total games processed: %d\n", *totalProcessed)
	mutex.Unlock()
}

// ParseGame from PGN
func parseGame(data string) *Game {
	game := &Game{}

	re := regexp.MustCompile(`\[(\w+) "([^"]*)"\]`)
	matches := re.FindAllStringSubmatch(data, -1)

	for _, match := range matches {
		tag := match[1]
		value := match[2]

		switch tag {
		case "Event":
			game.Event = value
		case "Site":
			game.Site = value
		case "Date":
			game.Date = value
		case "UTCTime":
			game.Time = value
		case "White":
			game.White = value
		case "Black":
			game.Black = value
		case "Result":
			game.Result = value
		case "WhiteElo":
			game.WhiteElo = convertToInt(value)
		case "BlackElo":
			game.BlackElo = convertToInt(value)
		case "ECO":
			game.Eco = value
		case "TimeControl":
			game.TimeControl = value
		case "Termination":
			game.Termination = value
		}
	}

	game.Moves = parseMovesFromPGN(data)
	game.MovesCount = getMoveCount(data)

	return game
}

func getMoveCount(data string) int {
	var moves []string

	// Regex to find moves
	re := regexp.MustCompile(`\d+\.\s+([^\d]+)`)
	matches := re.FindAllStringSubmatch(data, -1)

	for _, match := range matches {
		move := match[1]
		// Trim Spaces
		move = strings.TrimSpace(move)
		// Split moves
		moveParts := strings.Fields(move)
		for _, part := range moveParts {
			moves = append(moves, part)
		}
	}

	moveCount := len(moves)
	return moveCount - 1
}

func parseMovesFromPGN(gameString string) string {
	lines := strings.Split(gameString, "\n")
	var moves string
	for _, line := range lines {
		// Check start with int
		if len(line) > 0 && line[0] >= '0' && line[0] <= '9' {
			// Add to Line
			moves += line + " "
		}
	}
	return moves
}

// ConvertToInt
func convertToInt(s string) int {
	var n int
	fmt.Sscanf(s, "%d", &n)
	return n
}
