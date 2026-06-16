package file

import (
	"errors"
	"io"
	"os"
	"path/filepath"
)

type Handler struct {
	baseDir string
	locker  Locker
}

func NewFileHandler(baseDir string, locker Locker) (*Handler, error) {
	if baseDir == "" {
		return nil, errors.New("baseDir is empty")
	}

	err := os.MkdirAll(baseDir, 0755)
	if err != nil {
		return nil, err
	}

	return &Handler{
		baseDir: baseDir,
		locker:  locker,
	}, nil
}

func (f *Handler) Path(fileName string) string {
	path := filepath.Join(f.baseDir, fileName)
	return path
}

func (f *Handler) Exists(fileName string) bool {
	_, err := os.Stat(f.Path(fileName))
	return err == nil
}

func (f *Handler) CreateIfNotExists(fileName string) error {
	path := f.Path(fileName)

	_, err := os.Stat(path)
	if err == nil {
		return nil
	}

	if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return err
	}

	return file.Close()
}

func (f *Handler) Copy(srcFile, targetDir, newName string) error {
	f.locker.RLock(srcFile)
	defer f.locker.RUnlock(srcFile)
	// Ensure target directory exists
	err := os.MkdirAll(targetDir, os.ModePerm)
	if err != nil {
		return err
	}

	// Open source file
	src, err := os.Open(f.Path(srcFile))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if src == nil {
		// this is new session case
		return nil
	}
	defer src.Close()

	// Destination path
	dstPath := filepath.Join(targetDir, newName)

	// Create destination file
	// os.Create truncates if file already exists => overwrite
	dst, err := os.Create(dstPath)
	if err != nil {
		return err
	}
	defer dst.Close()

	// Copy content
	_, err = io.Copy(dst, src)
	return err
}

func (f *Handler) Write(fileName string, data []byte) error {
	f.locker.Lock(fileName)
	defer f.locker.Unlock(fileName)
	path := f.Path(fileName)
	return os.WriteFile(path, data, 0644)
}

func (f *Handler) Append(fileName string, data []byte) error {
	f.locker.Lock(fileName)
	defer f.locker.Unlock(fileName)
	path := f.Path(fileName)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Write(data)
	return err
}

func (f *Handler) Read(fileName string) ([]byte, error) {
	f.locker.RLock(fileName)
	defer f.locker.RUnlock(fileName)
	path := f.Path(fileName)

	return os.ReadFile(path)
}

func (f *Handler) Delete(fileName string) error {
	f.locker.Lock(fileName)
	defer f.locker.Unlock(fileName)
	path := f.Path(fileName)

	err := os.Remove(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}

	return err
}

func (f *Handler) DeleteAll() error {
	return os.RemoveAll(f.baseDir)
}

func (f *Handler) List() ([]string, error) {
	files, err := os.ReadDir(f.baseDir)
	if err != nil {
		return nil, err
	}
	list := make([]string, 0, len(files))
	for _, file := range files {
		list = append(list, file.Name())
	}
	return list, nil
}

func (f *Handler) Clear(fileName string) error {
	f.locker.Lock(fileName)
	defer f.locker.Unlock(fileName)
	path := f.Path(fileName)

	file, err := os.OpenFile(
		path,
		os.O_TRUNC|os.O_WRONLY|os.O_CREATE,
		0644,
	)
	if err != nil {
		return err
	}

	return file.Close()
}
