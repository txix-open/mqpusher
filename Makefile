build:
	go get github.com/mitchellh/gox
	gox -os="linux" -arch="amd64" -output="build/{{.OS}}_{{.Arch}}/{{.Dir}}"
	go mod tidy
