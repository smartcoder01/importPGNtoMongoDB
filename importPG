package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"gopkg.in/freeeve/pgn.v1"
)

type Game struct {
	Opening     string
	Eco         string
	Result      string
	White       string
	Black       string
	WhiteElo    int
	BlackElo    int
	Positions   []string // Storing positions as a slice of strings
	Moves       string
	MovesCount  int
	Event       string
	TimeControl string
	Termination string
	Date        time.Time
	Time        time.Time
	LichessId   string
}

func main() {
	if err := godotenv.Load(); err != nil {
		fmt.Println("No .env file found")
	}

	databaseUrl := os.Getenv("DATABASE_URL")
	folderPath := os.Getenv("FOLDER_PATH")

	pool, err := pgxpool.New(context.Background(), databaseUrl)
	if err != nil {
		fmt.Println("Failed to connect to PostgreSQL:", err)
		return
	}
	defer pool.Close()

	var wg sync.WaitGroup
	var mu sync.Mutex
	var totalGames int
	dirs := make(chan string, 10)

	// Add directories to the channel
	go func() {
		defer close(dirs)
		entries, err := os.ReadDir(folderPath)
		if err != nil {
			fmt.Println("Error reading directory:", err)
			return
		}
		for _, entry := range entries {
			if entry.IsDir() {
				dirs <- filepath.Join(folderPath, entry.Name())
			}
		}
	}()

	// Create workers to process directories in parallel
	for i := 0; i < 3; i++ { // Number of directory processing goroutines (for 2 directories)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for dirPath := range dirs {
				processDirectory(dirPath, pool, &totalGames, &mu)
			}
		}()
	}

	wg.Wait()
	fmt.Printf("Finished. Total Games Processed: %d\n", totalGames)
}

func processDirectory(dirPath string, pool *pgxpool.Pool, totalProcessed *int, mu *sync.Mutex) {
	var wg sync.WaitGroup
	files := make(chan string, 100)

	// Walk through the files in the directory and queue them for processing
	go func() {
		defer close(files)
		err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				fmt.Printf("Error accessing file %s: %s\n", path, err)
				return nil
			}

			if !info.IsDir() {
				files <- path
			}
			return nil
		})
		if err != nil {
			fmt.Println("Error processing files:", err)
		}
	}()

	tableName := strings.ReplaceAll(filepath.Base(dirPath), "-", "_")
	tableName = fmt.Sprintf("\"%s\"", tableName) // Ensure table name is valid

	// Create table for the current directory
	_, err := pool.Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			lichess_id TEXT UNIQUE,
			opening TEXT,
			eco TEXT,
			result TEXT,
			white TEXT,
			black TEXT,
			white_elo INTEGER,
			black_elo INTEGER,
			positions JSONB,
			moves TEXT,
			moves_count INTEGER,
			event TEXT,
			time_control TEXT,
			termination TEXT,
			date DATE,
			time TIME,
			created_at TIMESTAMPTZ DEFAULT now(),
			updated_at TIMESTAMPTZ DEFAULT now()
		);
	`, tableName))
	if err != nil {
		fmt.Printf("Failed to create table %s: %s\n", tableName, err)
		return
	}

	// Create workers to process files in the current directory
	for i := 0; i < 8; i++ { // Number of file processing goroutines
		wg.Add(1)
		go func() {
			defer wg.Done()
			for filePath := range files {
				processFile(filePath, pool, tableName, totalProcessed, mu)
			}
		}()
	}

	wg.Wait()
}

func processFile(filePath string, pool *pgxpool.Pool, tableName string, totalProcessed *int, mu *sync.Mutex) {
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("Failed to open file %s: %s\n", filePath, err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var gameData strings.Builder

	for scanner.Scan() {
		line := scanner.Text()

		if strings.HasPrefix(line, "[Event ") {
			if gameData.Len() > 0 {
				processGame(gameData.String(), pool, tableName)
				mu.Lock()
				*totalProcessed++
				fmt.Printf("Total games processed: %d\n", *totalProcessed)
				mu.Unlock()
				gameData.Reset()
			}
		}

		gameData.WriteString(line + "\n")
	}

	if gameData.Len() > 0 {
		processGame(gameData.String(), pool, tableName)
		mu.Lock()
		*totalProcessed++
		fmt.Printf("Total games processed: %d\n", *totalProcessed)
		mu.Unlock()
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading file %s: %s\n", filePath, err)
	}
}

func processGame(data string, pool *pgxpool.Pool, tableName string) {
	game := parseGame(data)

	positionsJSON, err := json.Marshal(game.Positions)
	if err != nil {
		fmt.Println("Failed to marshal positions to JSON:", err)
		return
	}

	_, err = pool.Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s (lichess_id, opening, eco, result, white, black, white_elo, black_elo, positions, moves, moves_count, event, time_control, termination, date, time)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
		ON CONFLICT (lichess_id) DO NOTHING
	`, tableName), game.LichessId, game.Opening, game.Eco, game.Result, game.White, game.Black, game.WhiteElo, game.BlackElo, positionsJSON, game.Moves, game.MovesCount, game.Event, game.TimeControl, game.Termination, game.Date, game.Time)

	if err != nil {
		fmt.Println("Failed to insert game into PostgreSQL:", err)
	}
}

func parseGame(data string) *Game {
	game := &Game{
		Positions: []string{}, // Initialize as a slice
	}

	re := regexp.MustCompile(`\[(\w+) "([^"]*)"\]`)
	matches := re.FindAllStringSubmatch(data, -1)

	for _, match := range matches {
		tag := match[1]
		value := match[2]

		switch tag {
		case "Opening":
			game.Opening = value
		case "Event":
			game.Event = value
		case "Site":
			game.LichessId = strings.TrimPrefix(value, "https://lichess.org/")
		case "Date":
			parsedDate, err := time.Parse("2006.01.02", value)
			if err == nil {
				game.Date = parsedDate
			}
		case "UTCTime":
			parsedTime, err := time.Parse("15:04:05", value)
			if err == nil {
				game.Time = parsedTime
			}
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
	game.Positions = parsePositionsFromPGN(data) // Now returns []string

	return game
}

func getMoveCount(data string) int {
	re := regexp.MustCompile(`\d+\.\s+([^\d]+)`)
	matches := re.FindAllStringSubmatch(data, -1)

	moves := make([]string, len(matches))
	for i, match := range matches {
		move := strings.TrimSpace(match[1])
		moveParts := strings.Fields(move)
		moves[i] = strings.Join(moveParts, " ")
	}

	return len(moves) - 1
}

func parseMovesFromPGN(gameString string) string {
	lines := strings.Split(gameString, "\n")
	var moves []string
	for _, line := range lines {
		if len(line) > 0 && line[0] >= '0' && line[0] <= '9' {
			moves = append(moves, line)
		}
	}

	movesStr := strings.Join(moves, " ")
	movesStr = regexp.MustCompile(`\{[^}]*\}|\b\d+\.|\d+-\d+|\.`).ReplaceAllString(movesStr, "")
	return strings.Join(strings.Fields(movesStr), " ")
}

func clearFromNotations(gameString string) string {
	// Remove all text inside curly braces including the braces
	re := regexp.MustCompile(`\{[^}]*\}`)
	cleanedString := re.ReplaceAllString(gameString, "")

	// Remove extra new lines or spaces
	cleanedString = strings.TrimSpace(cleanedString)
	cleanedString = strings.ReplaceAll(cleanedString, "\n\n", "\n")

	// Remove all patterns like " 1...", " 2...", ..., " n..."
	reNumber := regexp.MustCompile(` \d+\.\.\.`)
	cleanedString = reNumber.ReplaceAllString(cleanedString, "")

	// Remove extra spaces at the end
	cleanedString = strings.ReplaceAll(cleanedString, "  ", " ")
	cleanedString = strings.ReplaceAll(cleanedString, ". ", ".")

	// Remove trailing spaces
	cleanedString = strings.TrimSpace(cleanedString)

	return cleanedString
}

func parsePositionsFromPGN(data string) []string {
	// Clean the PGN data from notations
	cleanedData := clearFromNotations(data)

	ps := pgn.NewPGNScanner(strings.NewReader(cleanedData)) // Convert string to io.Reader

	var positions []string // Slice to store FEN positions

	// Iterate over all games in the PGN data
	for ps.Next() {
		// Scan the next game
		game, err := ps.Scan()
		if err != nil {
			fmt.Println("Failed with scanner:", err)
			continue
		}

		// Create a new board to get FEN positions
		b := pgn.NewBoard()
		for _, move := range game.Moves {
			// Make the move on the board
			b.MakeMove(move)
			// Get the FEN string for each move in the game
			fen := b.String()
			// Store the FEN string in the slice
			positions = append(positions, fen)
		}
	}

	return positions
}

func convertToInt(s string) int {
	var n int
	fmt.Sscanf(s, "%d", &n)
	return n
}
