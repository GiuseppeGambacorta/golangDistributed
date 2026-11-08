package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// ServiceInstance represents a registered service instance
type ServiceInstance struct {
	ID       string    `json:"id"`
	Service  string    `json:"service"`
	Address  string    `json:"address"`
	Port     string    `json:"port"`
	LastSeen time.Time `json:"last_seen"`
	Status   string    `json:"status"` // "healthy", "unhealthy"
}

// ServiceRegistry manages service discovery
type ServiceRegistry struct {
	services map[string][]*ServiceInstance // service name -> instances
	mu       sync.RWMutex
}

// NewServiceRegistry creates a new registry
func NewServiceRegistry() *ServiceRegistry {
	return &ServiceRegistry{
		services: make(map[string][]*ServiceInstance),
	}
}

// Register adds a service instance to the registry
func (sr *ServiceRegistry) Register(service, address, port string) string {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	instanceID := fmt.Sprintf("%s-%s-%d", service, address, time.Now().UnixNano())

	instance := &ServiceInstance{
		ID:       instanceID,
		Service:  service,
		Address:  address,
		Port:     port,
		LastSeen: time.Now(),
		Status:   "healthy",
	}

	sr.services[service] = append(sr.services[service], instance)
	fmt.Printf("Registered service: %s at %s:%s\n", service, address, port)

	return instanceID
}

// Deregister removes a service instance
func (sr *ServiceRegistry) Deregister(instanceID string) {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	for service, instances := range sr.services {
		for i, instance := range instances {
			if instance.ID == instanceID {
				sr.services[service] = append(instances[:i], instances[i+1:]...)
				fmt.Printf("Deregistered service: %s\n", instanceID)
				return
			}
		}
	}
}

// Discover finds all instances of a service
func (sr *ServiceRegistry) Discover(service string) []*ServiceInstance {
	sr.mu.RLock()
	defer sr.mu.RUnlock()

	return sr.services[service]
}

// Heartbeat updates the last seen time for an instance
func (sr *ServiceRegistry) Heartbeat(instanceID string) {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	for _, instances := range sr.services {
		for _, instance := range instances {
			if instance.ID == instanceID {
				instance.LastSeen = time.Now()
				instance.Status = "healthy"
				return
			}
		}
	}
}

// Cleanup removes stale instances
func (sr *ServiceRegistry) Cleanup() {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	for service, instances := range sr.services {
		validInstances := []*ServiceInstance{}
		for _, instance := range instances {
			if time.Since(instance.LastSeen) < 30*time.Second {
				validInstances = append(validInstances, instance)
			} else {
				fmt.Printf("Removing stale instance: %s\n", instance.ID)
			}
		}
		sr.services[service] = validInstances
	}
}

// StartHTTPServer starts the registry's HTTP API
func (sr *ServiceRegistry) StartHTTPServer(port string) {
	http.HandleFunc("/register", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req struct {
			Service string `json:"service"`
			Address string `json:"address"`
			Port    string `json:"port"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		instanceID := sr.Register(req.Service, req.Address, req.Port)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"id": instanceID})
	})

	http.HandleFunc("/discover/", func(w http.ResponseWriter, r *http.Request) {
		service := r.URL.Path[len("/discover/"):]
		instances := sr.Discover(service)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(instances)
	})

	http.HandleFunc("/heartbeat/", func(w http.ResponseWriter, r *http.Request) {
		instanceID := r.URL.Path[len("/heartbeat/"):]
		sr.Heartbeat(instanceID)
		w.WriteHeader(http.StatusOK)
	})

	// Start cleanup goroutine
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		for range ticker.C {
			sr.Cleanup()
		}
	}()

	fmt.Printf("Service registry running on port %s\n", port)
	http.ListenAndServe(":"+port, nil)
}
