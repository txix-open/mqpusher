package utils

type FileManager struct {
	files   []string
	nextIdx int
}

func NewFileManager(files []string) *FileManager {
	return &FileManager{
		files:   files,
		nextIdx: 0,
	}
}

func (f *FileManager) Next() (string, bool) {
	if f.nextIdx >= len(f.files) {
		return "", false
	}
	result := f.files[f.nextIdx]
	f.nextIdx++

	return result, true
}
