build:
	go get github.com/mitchellh/gox
	gox -os="linux windows" -arch="amd64" -output="build/{{.OS}}_{{.Arch}}/{{.Dir}}"
	go mod tidy
