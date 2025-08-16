// orchestrator/main.go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "kyrodbctl",
	Short: "KyroDB orchestrator CLI",
}

var engineAddr string
var authToken string

func init() {
	rootCmd.PersistentFlags().StringVarP(&engineAddr, "engine", "e", "http://localhost:3030", "Engine HTTP address")
	rootCmd.PersistentFlags().StringVar(&authToken, "auth-token", "", "Authorization token (adds 'Authorization: Bearer <token>')")
}

func addAuth(req *http.Request) {
	if authToken != "" {
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", authToken))
	}
}

// health command: GET /health
var healthCmd = &cobra.Command{
	Use:   "health",
	Short: "Check engine health",
	Run: func(cmd *cobra.Command, args []string) {
		url := fmt.Sprintf("%s/health", engineAddr)
		client := http.Client{Timeout: 2 * time.Second}
		req, _ := http.NewRequest("GET", url, nil)
		addAuth(req)
		resp, err := client.Do(req)
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

// offset command: GET /offset to fetch current log length
var offsetCmd = &cobra.Command{
	Use:   "offset",
	Short: "Get current log offset",
	Run: func(cmd *cobra.Command, args []string) {
		url := fmt.Sprintf("%s/offset", engineAddr)
		client := http.Client{Timeout: 2 * time.Second}
		req, _ := http.NewRequest("GET", url, nil)
		addAuth(req)
		resp, err := client.Do(req)
		if err != nil {
			fmt.Fprintf(os.Stderr, "❌ request failed: %v\n", err)
			os.Exit(1)
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			body, _ := io.ReadAll(resp.Body)
			fmt.Printf("⚠️  Engine returned %d: %s\n", resp.StatusCode, body)
			os.Exit(1)
		}
		var obj map[string]uint64
		if err := json.NewDecoder(resp.Body).Decode(&obj); err != nil {
			fmt.Fprintf(os.Stderr, "❌ invalid response: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("📦 Current offset = %d\n", obj["offset"])
	},
}

// snapshot command: POST /snapshot
var snapshotCmd = &cobra.Command{
	Use:   "snapshot",
	Short: "Trigger a full snapshot on the engine",
	Run: func(cmd *cobra.Command, args []string) {
		url := fmt.Sprintf("%s/snapshot", engineAddr)
		client := http.Client{Timeout: 5 * time.Second}
		req, _ := http.NewRequest("POST", url, nil)
		addAuth(req)
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

// compact command: POST /compact returns stats
var compactCmd = &cobra.Command{
	Use:   "compact",
	Short: "Run compaction (keep-latest) and snapshot",
	Run: func(cmd *cobra.Command, args []string) {
		url := fmt.Sprintf("%s/compact", engineAddr)
		client := http.Client{Timeout: 30 * time.Second}
		req, _ := http.NewRequest("POST", url, nil)
		addAuth(req)
		resp, err := client.Do(req)
		if err != nil {
			fmt.Fprintf(os.Stderr, "❌ request failed: %v\n", err)
			os.Exit(1)
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			body, _ := io.ReadAll(resp.Body)
			fmt.Printf("⚠️  Engine returned %d: %s\n", resp.StatusCode, body)
			os.Exit(1)
		}
		var out map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			fmt.Fprintf(os.Stderr, "❌ invalid response: %v\n", err)
			os.Exit(1)
		}
		stats := out["stats"]
		enc, _ := json.MarshalIndent(stats, "", "  ")
		fmt.Printf("✅ Compaction complete. Stats:\n%s\n", string(enc))
	},
}

func main() {
	// sql command: POST /sql {sql: "..."}
	sqlCmd := &cobra.Command{
		Use:   "sql",
		Short: "Execute a simple SQL statement",
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			stmt := args[0]
			url := fmt.Sprintf("%s/sql", engineAddr)
			payload := map[string]string{"sql": stmt}
			body, _ := json.Marshal(payload)
			client := http.Client{Timeout: 5 * time.Second}
			req, _ := http.NewRequest("POST", url, bytes.NewReader(body))
			req.Header.Set("content-type", "application/json")
			addAuth(req)
			resp, err := client.Do(req)
			if err != nil {
				fmt.Fprintf(os.Stderr, "❌ request failed: %v\n", err)
				os.Exit(1)
			}
			defer resp.Body.Close()
			io.Copy(os.Stdout, resp.Body)
			fmt.Println()
		},
	}

	// lookup command: GET /lookup?key=123
	lookupCmd := &cobra.Command{
		Use:   "lookup [key]",
		Short: "Lookup value by key",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			url := fmt.Sprintf("%s/lookup?key=%s", engineAddr, args[0])
			client := http.Client{Timeout: 2 * time.Second}
			req, _ := http.NewRequest("GET", url, nil)
			addAuth(req)
			resp, err := client.Do(req)
			if err != nil {
				fmt.Fprintf(os.Stderr, "❌ request failed: %v\n", err)
				os.Exit(1)
			}
			defer resp.Body.Close()
			if resp.StatusCode == 200 {
				io.Copy(os.Stdout, resp.Body)
				fmt.Println()
			} else {
				body, _ := io.ReadAll(resp.Body)
				fmt.Printf("⚠️  Engine returned %d: %s\n", resp.StatusCode, body)
				os.Exit(1)
			}
		},
	}

	// vector-insert: POST /vector/insert
	vecInsertCmd := &cobra.Command{
		Use:   "vector-insert [key] [comma-separated-floats]",
		Short: "Insert a vector",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			key := args[0]
			query := args[1]
			url := fmt.Sprintf("%s/vector/insert", engineAddr)
			// parse floats client-side
			vals := []float64{}
			for _, s := range bytes.Split([]byte(query), []byte(",")) {
				if len(bytes.TrimSpace(s)) == 0 {
					continue
				}
				var f float64
				if _, err := fmt.Sscan(string(s), &f); err == nil {
					vals = append(vals, f)
				}
			}
			payload := map[string]interface{}{"key": key, "vector": vals}
			body, _ := json.Marshal(payload)
			client := http.Client{Timeout: 5 * time.Second}
			req, _ := http.NewRequest("POST", url, bytes.NewReader(body))
			req.Header.Set("content-type", "application/json")
			addAuth(req)
			resp, err := client.Do(req)
			if err != nil {
				fmt.Fprintf(os.Stderr, "❌ request failed: %v\n", err)
				os.Exit(1)
			}
			defer resp.Body.Close()
			io.Copy(os.Stdout, resp.Body)
			fmt.Println()
		},
	}

	// vector-search: POST /vector/search
	vecSearchCmd := &cobra.Command{
		Use:   "vector-search [comma-separated-floats] [k]",
		Short: "Search nearest vectors",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			query := args[0]
			kVal, err := strconv.Atoi(args[1])
			if err != nil || kVal <= 0 {
				fmt.Fprintf(os.Stderr, "❌ invalid k: %v\n", args[1])
				os.Exit(1)
			}
			url := fmt.Sprintf("%s/vector/search", engineAddr)
			vals := []float64{}
			for _, s := range bytes.Split([]byte(query), []byte(",")) {
				if len(bytes.TrimSpace(s)) == 0 {
					continue
				}
				var f float64
				if _, err := fmt.Sscan(string(s), &f); err == nil {
					vals = append(vals, f)
				}
			}
			payload := map[string]interface{}{"query": vals, "k": kVal}
			body, _ := json.Marshal(payload)
			client := http.Client{Timeout: 5 * time.Second}
			req, _ := http.NewRequest("POST", url, bytes.NewReader(body))
			req.Header.Set("content-type", "application/json")
			addAuth(req)
			resp, err := client.Do(req)
			if err != nil {
				fmt.Fprintf(os.Stderr, "❌ request failed: %v\n", err)
				os.Exit(1)
			}
			defer resp.Body.Close()
			io.Copy(os.Stdout, resp.Body)
			fmt.Println()
		},
	}

	// rmi-build: POST /rmi/build (if feature enabled)
	rmiBuildCmd := &cobra.Command{
		Use:   "rmi-build",
		Short: "Build the RMI index (if supported)",
		Run: func(cmd *cobra.Command, args []string) {
			url := fmt.Sprintf("%s/rmi/build", engineAddr)
			client := http.Client{Timeout: 30 * time.Second}
			req, _ := http.NewRequest("POST", url, nil)
			addAuth(req)
			resp, err := client.Do(req)
			if err != nil {
				fmt.Fprintf(os.Stderr, "❌ request failed: %v\n", err)
				os.Exit(1)
			}
			defer resp.Body.Close()
			io.Copy(os.Stdout, resp.Body)
			fmt.Println()
		},
	}

	rootCmd.AddCommand(healthCmd, offsetCmd, snapshotCmd, compactCmd, sqlCmd, lookupCmd, vecInsertCmd, vecSearchCmd, rmiBuildCmd)
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
