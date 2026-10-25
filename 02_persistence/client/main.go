package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

func main() {
	serverAddr := "localhost:8080"

	conn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		fmt.Printf("Errore di connessione a %s: %v\n", serverAddr, err)
		return
	}
	defer conn.Close()

	fmt.Printf("Connesso al server su %s\n", serverAddr)
	fmt.Println("Comandi supportati: SET <key> <val>, GET <key>, DELETE <key>, KEYS,QUIT")
	fmt.Println("Scrivi 'exit' per chiudere il client.")
	fmt.Println("ma prima di tutto inserire la password")

	consoleReader := bufio.NewReader(os.Stdin)
	serverReader := bufio.NewReader(conn)

	for {
		fmt.Print("> ")
		input, err := consoleReader.ReadString('\n')
		if err != nil {
			fmt.Println("Errore lettura input:", err)
			break
		}

		input = strings.TrimSpace(input)
		if input == "" {
			continue
		}

		// Uscita locale dal client
		if strings.EqualFold(input, "exit") {
			fmt.Println("Chiusura client...")
			break
		}

		// Invia il comando al server con terminatore di riga
		_, err = fmt.Fprintf(conn, "%s\n", input)
		if err != nil {
			fmt.Println("Errore invio dati:", err)
			break
		}

		// Legge la risposta dal server
		responseBuffer := make([]byte, 1024)
		n, err := serverReader.Read(responseBuffer)
		if err != nil {
			fmt.Println("Connessione chiusa dal server o errore:", err)
			break
		}

		fmt.Print(string(responseBuffer[:n]))

		// Se abbiamo mandato QUIT, usciamo dal ciclo
		if strings.HasPrefix(strings.ToUpper(input), "QUIT") {
			break
		}
	}
}
