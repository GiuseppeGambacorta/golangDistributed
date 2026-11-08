package main

import (
	"fmt"
	"time"
)

func testServiceRegistry() {
	registry := NewServiceRegistry()

	// Start the registry server
	go registry.StartHTTPServer("8070")

	// Give server time to start
	time.Sleep(100 * time.Millisecond)

	// Test registration
	instanceID := registry.Register("kv-store", "localhost", "8080")
	fmt.Printf("Registered instance: %s\n", instanceID)

	// Test discovery
	instances := registry.Discover("kv-store")
	fmt.Printf("Discovered %d instances:\n", len(instances))
	for _, instance := range instances {
		fmt.Printf(" - %s at %s:%s\n", instance.ID, instance.Address, instance.Port)
	}

	// Keep running to see cleanup in action
	time.Sleep(35 * time.Second)

	instances = registry.Discover("kv-store")
	fmt.Printf("After cleanup: %d instances\n", len(instances))
}

func main() {
	testServiceRegistry()
}
