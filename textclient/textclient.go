package main

// Inspired by https://github.com/jhudson8/golang-chat-example/

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/jancona/m17text/m17"
)

var (
	serverArg     *string = flag.String("server", "", "Reflector server")
	portArg       *uint   = flag.Uint("port", 17000, "Port the reflector listens on")
	moduleArg     *string = flag.String("module", "T", "Module to connect to")
	callsignArg   *string = flag.String("callsign", "N0CALL", "User's callsign")
	helpArg       *bool   = flag.Bool("h", false, "Print arguments")
	listenArg     *bool   = flag.Bool("listen", false, "Start a TCP interface")
	listenPortArg *string = flag.String("listenport", "17001", "Port the client will listen on")
)

var encodedCallsign *[6]byte

var newM17Msgs = make(chan string)

func main() {
	flag.Parse()
	if *helpArg {
		flag.Usage()
		return
	}
	if *listenPortArg != "" && !*listenArg {
		fmt.Print("Warning: Listen port specified but listen not enabled\n")
	}

	var err error
	encodedCallsign, err = m17.EncodeCallsign(*callsignArg)
	if err != nil {
		fmt.Printf("Bad callsign %s: %v", *callsignArg, err)
		os.Exit(1)
	}
	r, err := m17.NewRelay(*serverArg, *portArg, *moduleArg, *callsignArg, handleM17)
	if err != nil {
		fmt.Printf("Error creating client: %v", err)
		os.Exit(1)
	}
	err = r.Connect()
	if err != nil {
		fmt.Printf("Error connecting to %s:%d %s: %v", *serverArg, *portArg, *moduleArg, err)
		os.Exit(1)
	}
	defer r.Close()

	// handle responses from reflector
	go func() {
		r.Handle()
		// When Handle exits, we're done
		close(newM17Msgs)
		os.Exit(0)
	}()

	if *listenArg {
		go startListener(*listenPortArg, r)
	}

	handleConsoleInput(r)
}

func handleM17(p m17.Packet) error {
	// // A packet is an LSF + type code 0x05 for SMS + data up to 823 bytes
	// log.Printf("[DEBUG] p: %#v", p)
	dst, err := m17.DecodeCallsign(p.LSF.Dst[:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Bad dst callsign: %v", err)
	}
	src, err := m17.DecodeCallsign(p.LSF.Src[:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Bad src callsign: %v", err)
	}
	var msg string
	if len(p.Payload) > 0 {
		msg = string(p.Payload[0 : len(p.Payload)-1])
	}
	if p.Type == m17.PacketTypeSMS && (dst == *callsignArg || dst == m17.DestinationAll || dst[0:1] == "#") {
		fmt.Printf("\n%s %s>%s: %s\n> ", time.Now().Format(time.DateTime), src, dst, msg)
		if *listenArg {
			newM17Msgs <- fmt.Sprintf("MSG%s,%s,%s,%s\n", time.Now().Format(time.DateTime), src, dst, msg)
		}
	}
	return nil
}

// keep watching for console input
// send the "message" command to the chat server when we have some
func handleConsoleInput(c *m17.Relay) {
	var done bool

	reader := bufio.NewReader(os.Stdin)

	for !done {
		print("> ")
		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Printf("Couldn't read from console: %v", err)
			os.Exit(1)
		}

		input = strings.TrimSpace(input)
		if input != "" {
			command, callsign, message, ok := parseInput(input)
			if !ok {
				// Ignore bad input
				fmt.Printf("Error parsing command \"%s\"\n", command)
				continue
			}

			if command == "" {
				// Add a trailing NUL
				msg := append([]byte(message), 0)
				p, err := m17.NewPacket(callsign, *callsignArg, m17.PacketTypeSMS, msg)
				if err != nil {
					fmt.Printf("Error creating Packet: %v\n", err)
					continue
				}
				err = c.SendPacket(*p)
				if err != nil {
					fmt.Printf("Error sending message: %v\n", err)
					continue
				}
			} else {
				switch command {
				case "quit":
					done = true

				default:
					fmt.Printf("Unknown command \"%s\"\n", command)
				}
			}
		}
	}
}

// Inputs are of the forms:
//
//	/command message
//	callsign: message
func parseInput(input string) (command, callsign, message string, ok bool) {
	if input[0] == '/' {
		// It's a command
		command, message, _ = strings.Cut(input[1:], " ")
		ok = true
		return
	}
	callsign, message, ok = strings.Cut(input, ": ")
	return
}

func startListener(port string, c *m17.Relay) {
	prefix := ":"
	formattedPort := prefix + port
	listener, err := net.Listen("tcp", formattedPort)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer listener.Close()

	fmt.Printf("Listening on port %s\n", *listenPortArg)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}
		//go handleConnection(conn, c) //weird behavior as channels are not parallel, so we cannot be either
		handleConnection(conn, c)
	}
}

func handleConnection(conn net.Conn, c *m17.Relay) {
	defer conn.Close()
	fmt.Printf("Connection established with %s\n", conn.RemoteAddr())
	_, err := conn.Write([]byte("200\n"))
	if err != nil {
		fmt.Println("Error writing:", err)
		return
	}

	go func(conn net.Conn) { // send new messages to them too
		for str := range newM17Msgs {
			_, err := conn.Write([]byte(str))
			if err != nil {
				fmt.Println("Error writing:", err)
			}
		}
	}(conn)

	reader := bufio.NewReader(conn)
	for {
		message, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading:", err)
			return
		}
		fmt.Printf("Received from %s: %s", conn.RemoteAddr(), message)
		command, callsign, message, ok := parseInput(message)
		if !ok {
			// Ignore bad input
			fmt.Printf("Error parsing command \"%s\"\n", command)
			_, err = conn.Write([]byte("400\n"))
			if err != nil {
				fmt.Println("Error writing:", err.Error())
				return
			}
			continue
		}
		if command == "" {
			// Add a trailing NUL
			msg := append([]byte(message), 0)
			p, err := m17.NewPacket(callsign, *callsignArg, m17.PacketTypeSMS, msg)
			if err != nil {
				fmt.Printf("Error creating Packet: %v\n", err)
				_, err = conn.Write([]byte("422\n"))
				if err != nil {
					fmt.Println("Error writing:", err.Error())
					return
				}
				continue
			}
			err = c.SendPacket(*p)
			if err != nil {
				fmt.Printf("Error sending message: %v\n", err)
				_, err = conn.Write([]byte("500\n"))
				if err != nil {
					fmt.Println("Error writing:", err.Error())
					return
				}
				continue
			}
			_, err = conn.Write([]byte("200\n"))
			if err != nil {
				fmt.Println("Error writing:", err.Error())
				return
			}
		}
	}

}
