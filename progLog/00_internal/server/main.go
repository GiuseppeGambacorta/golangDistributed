package main

import (
	"log"
)

func main() {
	srv := NewHTTPServer(":8080")
	log.Fatal(srv.ListenAndServe())
}
