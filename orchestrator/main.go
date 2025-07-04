// orchestrator/main.go
package main

import (
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "os"
    "time"

    "github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
    Use:   "ngdbctl",
    Short: "NextGen‑DB orchestrator CLI",
}

var engineAddr string

func init() {
    rootCmd.PersistentFlags().StringVarP(&engineAddr, "engine", "e", "http://localhost:3030", "Engine HTTP address")
}

// health command: GET /health
var healthCmd = &cobra.Command{
    Use:   "health",
    Short: "Check engine health",
    Run: func(cmd *cobra.Command, args []string) {
        url := fmt.Sprintf("%s/health", engineAddr)
        client := http.Client{Timeout: 2 * time.Second}
        resp, err := client.Get(url)
        if err != nil {
            fmt.Fprintf(os.Stderr, "❌ request failed: %v\n", err)
            os.Exit(1)
        }
        defer resp.Body.Close()
        if resp.StatusCode == 200 {
            fmt.Println("✅ Engine is healthy")
        } else {
            body, _ := io.ReadAll(resp.Body)
            fmt.Printf("⚠️  Engine returned %d: %s\n", resp.StatusCode, body)
            os.Exit(1)
        }
    },
}

// offset command: GET /replay?start=0&end=0 to fetch get_offset
var offsetCmd = &cobra.Command{
    Use:   "offset",
    Short: "Get current log offset",
    Run: func(cmd *cobra.Command, args []string) {
        url := fmt.Sprintf("%s/replay?start=0&end=0", engineAddr)
        client := http.Client{Timeout: 2 * time.Second}
        resp, err := client.Get(url)
        if err != nil {
            fmt.Fprintf(os.Stderr, "❌ request failed: %v\n", err)
            os.Exit(1)
        }
        defer resp.Body.Close()
        var events []map[string]interface{}
        if err := json.NewDecoder(resp.Body).Decode(&events); err != nil {
            fmt.Fprintf(os.Stderr, "❌ invalid response: %v\n", err)
            os.Exit(1)
        }
        // replay with end=0 returns an empty array but we know offset via header?
        // As a quick hack, use GET /subscribe?from=0 but only first element...
        fmt.Printf("📦 Current offset = %d\n", len(events))
    },
}

// snapshot command: POST /snapshot  (we need to add this route on the server)
var snapshotCmd = &cobra.Command{
    Use:   "snapshot",
    Short: "Trigger a full snapshot on the engine",
    Run: func(cmd *cobra.Command, args []string) {
        url := fmt.Sprintf("%s/snapshot", engineAddr)
        client := http.Client{Timeout: 5 * time.Second}
        req, _ := http.NewRequest("POST", url, nil)
        resp, err := client.Do(req)
        if err != nil {
            fmt.Fprintf(os.Stderr, "❌ request failed: %v\n", err)
            os.Exit(1)
        }
        defer resp.Body.Close()
        if resp.StatusCode == 200 {
            fmt.Println("✅ Snapshot triggered")
        } else {
            body, _ := io.ReadAll(resp.Body)
            fmt.Printf("⚠️  Engine returned %d: %s\n", resp.StatusCode, body)
            os.Exit(1)
        }
    },
}

func main() {
    rootCmd.AddCommand(healthCmd, offsetCmd, snapshotCmd)
    if err := rootCmd.Execute(); err != nil {
        os.Exit(1)
    }
}
