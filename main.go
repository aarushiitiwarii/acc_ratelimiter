package main

import (
	"fmt"
	"net/http"
	"os"
)

func main() {
	host, _ := os.Hostname()

	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	})

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "hello from %s\n", host)
	})

	fmt.Println("listening on :8080")
	http.ListenAndServe(":8080", nil)
}