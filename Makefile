test:
	go test -cover ./...

cover:
	go test -coverprofile cover.out  ./... && go tool cover -html=cover.out

race:
	go test -race -cpu=1,4,32,100 -count=10 -failfast ./...
