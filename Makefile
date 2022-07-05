all:
	ls * | entr go test -v *.go

bench:
	go test -bench=.

cover:
	go test -cover
