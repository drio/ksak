all:
	ls * | entr go test -v *.go

bench:
	go test -bench=.

cover:
	go test -cover

release:
	@echo -e "New release version:";\
		read VERSION;\
		echo $$VERSION
