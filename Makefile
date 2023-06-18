.PHONY: test
test:
	go test -run=^Test -race -cover .

.PHONY: bench
bench:
	go test -run=^$$ -cover -bench .

.PHONY: security
security:
	@echo "> Racing testing..."
	@go test -race -cover .
	@echo -e "\n> Racing benchmarks..."
	@go test -run=^$$ -race -cover -bench .
	@echo -e "\n> Running gosec..."
	@gosec .

.PHONY: clean
clean:
	rm -f *.out
	rm -f *.tmp
	go clean
	go fmt .
	go vet .
