package main

import (
	"fmt"
	"os"
	"bufio"

	"github.com/tylerchr/parallel-database/query/parser"
)

func main() {

	reader := bufio.NewReader(os.Stdin)

	for {

			fmt.Print("> ")
			text, _ := reader.ReadString('\n')

			if text == "goodbye\n" {
					fmt.Println("Goodbye!")
					break
			}

			if q, err := parser.ParseQuery(text); err != nil {
				fmt.Printf("[error] %v\n", err)
			} else {
				fmt.Printf("[parsed] %v\n", q)
			}

	}

}