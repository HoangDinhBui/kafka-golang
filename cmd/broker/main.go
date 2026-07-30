package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/HoangDinhBui/kafka-golang/internal/config"
	"github.com/HoangDinhBui/kafka-golang/internal/server"
	"github.com/HoangDinhBui/kafka-golang/internal/ui"
)

func main() {
	// Parse CLI flags
	portFlag := flag.String("port", "9092", "TCP listener port for Kafka broker")
	dataDirFlag := flag.String("data-dir", "./data", "Directory path for log storage")
	nodeIdFlag := flag.Int("node-id", 1, "Unique integer Node ID for this broker")
	hostFlag := flag.String("host", "127.0.0.1", "Broker host or IP address")
	uiPortFlag := flag.Int("ui-port", 8080, "Port for Web Management UI (pass 0 to disable)")
	flag.Parse()

	// Initialize configuration
	cfg := config.DefaultConfig()
	cfg.Port = *portFlag
	cfg.DataDir = *dataDirFlag

	portInt, err := strconv.Atoi(cfg.Port)
	if err != nil {
		log.Fatalf("Invalid port number: %s", cfg.Port)
	}

	// Initialize request router handler & TCP server
	handler := server.NewHandler(cfg.DataDir, int32(*nodeIdFlag), *hostFlag, int32(portInt))
	bindAddr := fmt.Sprintf(":%s", cfg.Port)
	tcpServer := server.NewTCPServer(bindAddr, handler)

	var uiServer *ui.UIServer
	if *uiPortFlag > 0 {
		uiServer = ui.NewServer(handler, *uiPortFlag)
	}

	// Print startup banner
	fmt.Println("================================================================")
	fmt.Println("  Apache Kafka Clone in Go (kafka-golang)")
	fmt.Printf("  - Node ID   : %d\n", *nodeIdFlag)
	fmt.Printf("  - Address   : %s:%s\n", *hostFlag, cfg.Port)
	fmt.Printf("  - Data Dir  : %s\n", cfg.DataDir)
	if *uiPortFlag > 0 {
		fmt.Printf("  - Web UI    : http://localhost:%d\n", *uiPortFlag)
	} else {
		fmt.Println("  - Web UI    : DISABLED")
	}
	fmt.Println("  - Status    : READY & LISTENING FOR CLIENTS")
	fmt.Println("================================================================")

	// Channel for catching OS signals (SIGINT, SIGTERM)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	// Start TCP server in a background goroutine
	serverErrChan := make(chan error, 1)
	go func() {
		if err := tcpServer.Start(); err != nil {
			serverErrChan <- err
		}
	}()

	// Start Web UI Server if enabled
	if uiServer != nil {
		go func() {
			if err := uiServer.Start(); err != nil {
				log.Printf("Web UI Server error: %v\n", err)
			}
		}()
	}

	// Wait for termination signal or server error
	select {
	case err := <-serverErrChan:
		log.Fatalf("TCP Server error: %v", err)
	case sig := <-sigChan:
		fmt.Printf("\n[Signal Received: %v] Shutting down Kafka Broker gracefully...\n", sig)
		if uiServer != nil {
			if err := uiServer.Stop(); err != nil {
				log.Printf("Error stopping Web UI server: %v\n", err)
			}
		}
		if err := tcpServer.Stop(); err != nil {
			log.Printf("Error stopping TCP server: %v\n", err)
		}
		fmt.Println("[Shutdown Complete] Goodbye!")
	}
}

